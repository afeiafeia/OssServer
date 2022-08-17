package dispatch

import (
	"fmt"
	"net"
	"oss/app"
	"oss/codec"
	"oss/dao/influxdb"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time"

	"github.com/spf13/viper"
)

//以下四项数据结构用于对应配置文件中的数据
type DataHandleInfo struct {
	Ip string `json:"ip"` //实际是ip+port
}

type DataHandleSet struct {
	DataHandle []DataHandleInfo `json:"dataHandle"`
}

type NodeConfigInfo struct {
	IpPort    string `json:"ipPort"` //ip:port形式
	Root      string `json:"root"`
	Space     string `json:"space"`
	SpaceUnit string `json:"spaceUnit"`
	BlockSize string `json:"blockSize"`
	BlockUnit string `json:"blockUnit"`
}

type NodeConfigInfoSet struct {
	Node []NodeConfigInfo `json:"node"`
}

var SimpleTimer *time.Timer

const (
	simpleTime    = 10
	simplePriodic = 10
)

type Dispatch struct {
	//其所持有的唯一连接是Dispatch
	// 底层服务框架
	base *base.Base

	//时序数据库influxdb
	influxdb_client *influxdb.InfluxdbApi

	//额外记录dataHandle的连接句柄
	//在转发由client发往dataHandle的报文时使用,目前只会有一个dataHandle的连接
	dataHandleClients map[string]*base.Session
	//额外记录dataHandle的连接句柄，key是节点的id
	//在转发由dataHandle发往node的报文是使用
	nodeSession map[uint64]*base.Session
	//互斥锁
	mu sync.Mutex

	//上传对象时对象信息的缓存，将在每个对象上传完成或者超时后清除对象对应的项
	objectSaveStrategyMap map[string]*ObjectSaveStrategy //key是对象的唯一id

	//下载对象时对象信息的缓存，将在每个对象下载完成或者超时后清除对象对应的项
	objectQueryResMap map[string]*ObjectQueryRes //key是对象的唯一id

	//用于等待跟dataHandle同步dataSaveSvr的信息
	syncNodeInfoChan chan int64

	//QPS统计相关
	SimpleClientInfoTime  time.Time
	clientReqCount        int64 //客户端请求的数量
	clientRspCount        int64 //dispatch回复客户端的回复数量
	clientRspSuccessCount int64 //成功处理的客户端请求的数量
}

//对象上传时，对象相关信息的缓存的数据结构
type ObjectSaveStrategy struct {
	mu       sync.Mutex
	objectId string //对象的唯一id

	objectSlicePlanChan chan int8                 //分片获取的结果，当收到dataHandle发送过来的分片时，通知主协程
	objectSlicePlan     *app.ObjectStoragePlanRsp //分片的落地方案
	objectSliceData     *app.ObjectSliceUploadReq //分片的数据

	objectSliceCount        uint32          //改对象分片的数量
	objectSaveResEverySlice map[uint32]int8 //每一个分片的在节点node的落盘的结果，1表示成功，-1表示失败，0表示暂未处理，key时分片的id
	objectSaveResChan       chan int8       //处理结果，可读时，表示所有的分片都处理完(可能有的成功，有的失败，需要利用ObjectSaveResEverySlice分析具体情况)
	objectSaveFinishChan    chan int8       //对象上传流程结束

	version int32 //分片方案的版本,用于区分一个对象在有分片落盘失败时，dataHandle调整后的分片方案与之前的分片方案

	tmpObjData *[]byte //当收到client发送过来的对象数据时，暂存到此，等请求到分片数据后，构造成objectSliceData
}

//递增收到的client端请求的数量
func (dispatch *Dispatch) IncreaseReqCount() {
	dispatch.mu.Lock()
	dispatch.clientReqCount++
	dispatch.mu.Unlock()
}

//递增回复给client的响应的数量
func (dispatch *Dispatch) IncreaseRspCount() {
	dispatch.mu.Lock()
	dispatch.clientRspCount++
	dispatch.mu.Unlock()
}

type ObjectQueryRes struct {
	mu                         sync.Mutex
	sliceInfo                  *app.ObjectLocationRsp //对象的分片落地方案
	objectSliceCount           uint32                 //对象的分片总数量(原始数据+EC码冗余数据)
	objectSliceNextIndex       uint32                 //当前可获取的下一个分片的下标
	objectOriDataSliceCount    uint32                 //该对象原始数据分片的数量，每收到一个确认回复，该值减1，减为0表示所有的数据都已经获取
	objectMetadataQueryResChan chan int8              //对于该对象的元数据查询的结果
	objectQueryResChan         chan int8              //处理结果，可读时，表示所有的分片都处理完(可能有的成功，有的失败，需要利用ObjectSaveResEverySlice分析具体情况)
}

const (
	TIME_OUT             = 2 * time.Second //超时时间
	TIME_OUT_RETRY_TIMES = 5               //重连次数
)

func (dispatch *Dispatch) Init(b *base.Base, dataHanldeSet DataHandleSet, nodeSet NodeConfigInfoSet) error {
	dispatch.base = b

	dispatch.dataHandleClients = make(map[string]*base.Session)
	dispatch.nodeSession = make(map[uint64]*base.Session)
	dispatch.objectSaveStrategyMap = make(map[string]*ObjectSaveStrategy)
	dispatch.objectQueryResMap = make(map[string]*ObjectQueryRes)

	//连接dataHandle
	err := dispatch.ConnectToDataHandle(dataHanldeSet)
	if err != nil {
		log.Error("connect to datahandle failed: %v", err)
		return err
	}

	//连接node
	err = dispatch.ConnectToNode(nodeSet)
	if err != nil {
		log.Error("connect to node failed: %v", err)
		return err
	}

	dispatch.connectToInfluxDB()
	log.Info("Connect to influxdb success")
	//要进行一个节点同步操作：从tcaplus中获取节点信息，与连接的情况对比

	//周期性统计并上传全链路QPS、流量的数据信息
	expire := QPSSimplePeriodic * time.Second
	QPSTimer = time.AfterFunc(expire, dispatch.uploadSimpleQPSInfo)

	//周期性统计服务处理的client端的请求的数量、结果以及QPS的信息
	dispatch.SimpleClientInfoTime = time.Now()
	go dispatch.uploadClientReqRspInfo()
	expire = ClientSimplePeriodic * time.Second
	ClientInfoTimer = time.AfterFunc(expire, dispatch.uploadClientReqRspInfo)

	return nil
}

func (dispatch *Dispatch) ConnectToDataHandle(dataHanldeSet DataHandleSet) error {
	if len(dataHanldeSet.DataHandle) == 0 {
		return fmt.Errorf("invalid input")
	}
	for _, dataHandler := range dataHanldeSet.DataHandle {

		connector, err := net.Dial("tcp", dataHandler.Ip)
		if err != nil {
			log.Error("connect to datahandle: %v failed: %v", dataHandler, err)
			continue
		}
		sessionIndex := "DataHandle"
		session := base.CreateSession(dispatch.base, &connector, base.CLIENT_SESSION, sessionIndex)
		dispatch.dataHandleClients[sessionIndex] = session
		dispatch.base.AddSession(session)
		log.Info("connect to DataHandle: %v success", dataHandler)
	}
	if len(dispatch.dataHandleClients) == 0 {
		log.Error("connect to datahandle all failed\n")
		return fmt.Errorf("connect to datahandle all failed\n")
	}
	return nil
}

func (dispatch *Dispatch) ConnectToNode(nodeSet NodeConfigInfoSet) error {
	if len(nodeSet.Node) == 0 {
		return fmt.Errorf("invalid input")
	}
	aliveNode := app.NodeInfoUpdateReq{}
	aliveNode.Node = make([]app.NodeInfo, 0)
	//暂时记录一个ip+port到session的映射
	tmpSessionMap := make(map[string]*base.Session)
	for _, nodeConfigInfo := range nodeSet.Node {
		connector, err := net.Dial("tcp", nodeConfigInfo.IpPort)
		if err != nil {
			log.Error("connect to datahandle: %v failed: %v", nodeConfigInfo, err)
			continue
		}
		//对于node的连接，其session的index是node的id，将记录到tcaplus中
		key := nodeConfigInfo.IpPort
		session := base.CreateSession(dispatch.base, &connector, base.CLIENT_SESSION, key)
		log.Debug("Create session:%v by index: %v", session, key)
		tmpSessionMap[key] = session
		//base将开始监视session上的到达请求
		dispatch.base.AddSession(session)
		//连接node成功后，需要向dataHandle发送节点信息
		addr := strings.Split(nodeConfigInfo.IpPort, ":")
		ip := addr[0]
		port, _ := strconv.Atoi(addr[1])
		space, _ := strconv.Atoi(nodeConfigInfo.Space)
		block, _ := strconv.Atoi(nodeConfigInfo.BlockSize)
		nodeInfoReq := app.NodeInfo{
			Ip:        ip,
			Port:      (uint32)(port),
			Root:      nodeConfigInfo.Root,
			Space:     TansToByteUnit((uint64)(space), nodeConfigInfo.SpaceUnit),
			BlockSize: TansToByteUnit((uint64)(block), nodeConfigInfo.BlockUnit),
		}
		aliveNode.Node = append(aliveNode.Node, nodeInfoReq)

		//后续添加
		//首先应该判断是不是已经存在的节点，如果是，则进行tcaplus中的更新，否则记录到tcaplus中
		log.Info("connect to Node: %v success", nodeConfigInfo)
	}
	if len(tmpSessionMap) == 0 {
		log.Error("connect to datahandle all failed")
		return fmt.Errorf("connect to datahandle all failed\n")
	}
	dispatch.syncNodeInfoChan = make(chan int64, len(aliveNode.Node))
	defer func() {
		dispatch.mu.Lock()
		if dispatch.syncNodeInfoChan != nil {
			close(dispatch.syncNodeInfoChan)
			dispatch.syncNodeInfoChan = nil
		}
		dispatch.mu.Unlock()
	}()
	err := dispatch.sendAddNodeInfo(&aliveNode)
	if err != nil {
		log.Error("Sync node info failed: %v", err)
		return err
	}
	return nil
}

//在连接上dataHandle以及Node后，将所连接的node信息发送给dataHandle

//向dataHandle发送节点初始化信息
func (dispatch *Dispatch) sendAddNodeInfo(req *app.NodeInfoUpdateReq) error {

	//构造头部
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding bucketInfo: %v failed: %v", *req, err)
		return fmt.Errorf("Marshal node info body to add failed: %v", err)
	}
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("AddNodeInfo")
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		return fmt.Errorf("Marshal node info header to add failed: %v", err)
	}
	//发送给任一一个连接的dataHandle
	for _, s := range dispatch.dataHandleClients {
		err := s.Write(headerBytes, payloadBytes)
		if err != nil {
			log.Warn("send node info to datahandle: %v failed: %v", *s, err)
			continue
		}
		break
	}

	//此处等待，直到收到回复或者超时
	timeOutChan := time.After(5 * time.Second)
	select {
	case <-timeOutChan:
		log.Error("sync node info time out")
		return fmt.Errorf("sync node info time out")
	case res := <-dispatch.syncNodeInfoChan:
		if res == -1 {
			log.Error("sync info failed")
			return fmt.Errorf("sync info failed")
		}
	}
	return nil
}

func TansToByteUnit(size uint64, unit string) uint64 {
	KB := "KB"
	MB := "MB"
	GB := "GB"
	TB := "TB"

	unit = strings.ToUpper(unit)
	if strings.Compare(unit, KB) == 0 {
		return size * 1024
	} else if strings.Compare(unit, MB) == 0 {
		return size * 1024 * 1024
	} else if strings.Compare(unit, GB) == 0 {
		return size * 1024 * 1024 * 1024
	} else if strings.Compare(unit, TB) == 0 {
		return size * 1024 * 1024 * 1024 * 1024
	} else {
		return 0
	}
}

func (dispatch *Dispatch) GetInfluxdbClient() *influxdb.InfluxdbApi {
	return dispatch.influxdb_client
}

//influxdb连接时，不存在连接不上的情况
//即使连接参数不对，db_handler.influxdb_client及其client也均不为空，但后期会在调用接口时返回连接失败的错误
//因此influxdb无法通过连接的结果判断是否连接成功，所有influxdb不使用重连机制
func (dispatch *Dispatch) connectToInfluxDB() {
	dispatch.influxdb_client = influxdb.NewInfludbClient(
		viper.GetString("DAO.INFLUXDB.IN_OGR"),
		viper.GetString("DAO.INFLUXDB.IN_TOKEN"),
		viper.GetString("DAO.INFLUXDB.IN_ADDR"),
		viper.GetString("DAO.INFLUXDB.IN_BUCKET"))

	return
}
