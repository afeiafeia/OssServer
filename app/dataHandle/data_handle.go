package dataHandle

import (
	"oss/dao/tcaplus/tcaplus_uecqms"
	"sync"
	"time"
	_ "time"

	"oss/app"
	_ "oss/app"
	"oss/dao/influxdb"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"

	"github.com/spf13/viper"
)

var DeleteTimer *time.Timer
var UserCountTimer *time.Timer

type DataHandle struct {
	//其所持有的唯一连接是Dispatch
	// 底层服务框架
	base *base.Base
	//tcaplus客户端，访问slice级别的表的客户端
	tcaplus_client *tcaplus_api.TcaplusApi

	//访问object及以上级别的表的tcaplus客户端
	tcaplus_client_object *tcaplus_api.TcaplusApi
	//时序数据库influxdb
	influxdb_client *influxdb.InfluxdbApi
	//互斥锁
	mu sync.Mutex

	//每一个对象的分片的落地结果
	sliceChans map[string](map[uint32]chan uint32)

	//key是objectId
	objectDataInfoMap map[string]*ObjectDataInfo

	//objId到对应dispatch连接的映射
	objectDispatchMap map[string]*base.Session

	//当前存活的节点,key是id
	aliveNodeMap map[uint64]*tcaplus_uecqms.Tb_Node_Info

	//目前系统中存在的bucketId,key是bucketId,value是该bucket中的对象数量
	allUserIdBucketIdMap map[string](map[string]uint64)
}

type ObjectDataInfo struct {
	mu         sync.Mutex
	metaData   *app.ObjectMetadataUploadReq        //对象的元数据
	sliceInfo  map[uint32]*app.ObjectSliceLocation //对象的分片方案
	retryTimes uint8                               //重传次数，当将为0后，停止重传
	stopChan   chan int8                           //1表示处理成功，-1表示处理失败
}

const (
	TIME_OUT             = 2 * time.Second //超时时间
	TIME_OUT_RETRY_TIMES = 5               //重传次数
)

// 服务初始化:连接数据库
func (dataHandler *DataHandle) Init(s *base.Base) error {

	dataHandler.sliceChans = make(map[string]map[uint32]chan uint32)
	dataHandler.objectDataInfoMap = make(map[string]*ObjectDataInfo)
	dataHandler.objectDispatchMap = make(map[string]*base.Session)

	//存活的存储节点
	dataHandler.aliveNodeMap = make(map[uint64]*tcaplus_uecqms.Tb_Node_Info)

	//所存在的bucket,没当有用户连接到了时，更新一下
	dataHandler.allUserIdBucketIdMap = make(map[string](map[string]uint64))

	var err error
	dataHandler.base = s
	err = dataHandler.connectToTcaplusDB()
	if err != nil {
		//后续应该加上重试机制，不能因为后端服务不能使用导致此服务启动失败
		//方案：连接失败时重新尝试连接，持续一段时间仍连接不上再退出
		err = dataHandler.retryConnectToTcaplusDB()
	}
	if err != nil {
		log.Error("connect to tcaplus failed")
		return err
	}
	log.Info("Connect to tcaplus success")

	dataHandler.connectToInfluxDB()
	log.Info("Connect to influxdb success")

	//开启定时器，周期性进行碎片整理(暂未实现完)
	expire := calExpireTime()
	DeleteTimer = time.AfterFunc(expire, dataHandler.deleteBucketPeriodically)

	//周期性上报更新服务所注册的用户数量
	go dataHandler.uploadUserCount()
	expire = calUserCountUpdataExpireTime()
	UserCountTimer = time.AfterFunc(expire, dataHandler.uploadUserCount)

	//周期性统计、上报tcaplus的访问QPS
	expire = TcaplusSimpleTime * time.Second
	TcaplusAccessInfoSimple = time.AfterFunc(expire, dataHandler.UploadTcaplusAccessInfo)

	//周期性统计、上报服务节点全链路QPS、流量的数据信息
	expire = QPSSimplePeriodic * time.Second
	QPSTimer = time.AfterFunc(expire, dataHandler.uploadSimpleQPSInfo)

	return nil
}

func calExpireTime() time.Duration {
	now := time.Now().Local()
	next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+8, 0, 0, 0, time.Local)
	return next.Sub(now)
}

//每2分钟刷新一次
func calUserCountUpdataExpireTime() time.Duration {
	now := time.Now().Local()
	next := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()+2, 0, 0, time.Local)
	return next.Sub(now)
}
func (data_handler *DataHandle) GetTcaplusClient() *tcaplus_api.TcaplusApi {
	return data_handler.tcaplus_client
}

func (data_handler *DataHandle) GetTcaplusClientObject() *tcaplus_api.TcaplusApi {
	return data_handler.tcaplus_client_object
}

func (data_handler *DataHandle) GetInfluxdbClient() *influxdb.InfluxdbApi {
	return data_handler.influxdb_client
}

func (data_handler *DataHandle) connectToTcaplusDB() error {
	var err error
	//tcaplus连接时
	//经测试，如果连接不上，会阻塞一段时间，其内部应该是有超时重连机制
	data_handler.tcaplus_client, err = tcaplus_api.NewTcaplusClent(
		uint64(viper.GetInt("DAO.TCAPLUS.TC_AppId")),
		uint32(viper.GetInt("DAO.TCAPLUS.TC_ZoneId")),
		viper.GetString("DAO.TCAPLUS.TC_DirUrl"),
		viper.GetString("DAO.TCAPLUS.TC_Signature"),
		viper.GetString("DAO.TCAPLUS.LOG_CONFIG"))
	if err != nil {
		log.Error("connect to tcaplus failed %v", err)
		return err
	}

	//访问bucket级别的表的客户端
	//经测试，如果连接不上，会阻塞一段时间，其内部应该是有超时重连机制
	data_handler.tcaplus_client_object, err = tcaplus_api.NewTcaplusClent(
		uint64(viper.GetInt("DAO.TCAPLUS.TC_AppId")),
		uint32(viper.GetInt("DAO.TCAPLUS.TC_ZoneId")),
		viper.GetString("DAO.TCAPLUS.TC_DirUrl"),
		viper.GetString("DAO.TCAPLUS.TC_Signature"),
		viper.GetString("DAO.TCAPLUS.LOG_CONFIG"))
	if err != nil {
		log.Error("connect to tcaplus failed %v", err)
		return err
	}
	return nil
}

func (data_handler *DataHandle) retryConnectToTcaplusDB() error {

	var err error
	for i := 0; i < TIME_OUT_RETRY_TIMES; i++ {
		time.Sleep(time.Duration(i * int(TIME_OUT)))
		err = data_handler.connectToTcaplusDB()
		if err == nil {
			break
		}
	}
	return err
}

//influxdb连接时，不存在连接不上的情况
//即使连接参数不对，db_handler.influxdb_client及其client也均不为空，但后期会在调用接口时返回连接失败的错误
//因此influxdb无法通过连接的结果判断是否连接成功，所有influxdb不使用重连机制
func (data_handler *DataHandle) connectToInfluxDB() {
	data_handler.influxdb_client = influxdb.NewInfludbClient(
		viper.GetString("DAO.INFLUXDB.IN_OGR"),
		viper.GetString("DAO.INFLUXDB.IN_TOKEN"),
		viper.GetString("DAO.INFLUXDB.IN_ADDR"),
		viper.GetString("DAO.INFLUXDB.IN_BUCKET"))

	return
}
