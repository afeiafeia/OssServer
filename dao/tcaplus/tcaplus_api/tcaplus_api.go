package tcaplus_api

//tcaplus的底层操作的封装：创建客户端连接、增删查改
import (
	"fmt"
	"oss/lib/log"
	_ "strings"
	"sync"
	"time"

	tcaplus "git.woa.com/gcloud_storage_group/tcaplus-go-api"
	c "git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"git.woa.com/gcloud_storage_group/tcaplus-go-api/record"
	"git.woa.com/gcloud_storage_group/tcaplus-go-api/request"
	"git.woa.com/gcloud_storage_group/tcaplus-go-api/response"
	"git.woa.com/gcloud_storage_group/tcaplus-go-api/terror"
	"github.com/rangechow/errors"
)

const RECV_TIMEOUT = 10 // 超时间 5秒

const (
	NO_RECORD errors.ErrCode = iota
)

type AccessInfomation struct {
	StartTime time.Time
	EndTime   time.Time
	Count     int64
}

type TcaplusApi struct {

	// tcaplus分配的app id
	appId uint64
	// app里的大区信息
	zoneId uint32
	// tcaplus为大区分配的目录服务地址
	dirUrl string
	// app的密码
	signature string

	// tcaplus client的句柄
	client *tcaplus.Client

	// 锁
	mu sync.Mutex
	// 接收通道,每一个对数据库的读写请求对应一个通道，
	chans map[uint64](chan response.TcaplusResponse)

	count int64 //调用次数

	tsid uint64

	accessInfo AccessInfomation
}

func NewTcaplusClent(AppId uint64, ZoneId uint32, DirUrl, Signature, ConfigPath string) (*TcaplusApi, error) {

	var t *TcaplusApi = new(TcaplusApi)

	t.accessInfo.Count = 0
	t.accessInfo.StartTime = time.Now()
	t.appId = AppId
	t.zoneId = ZoneId
	t.dirUrl = DirUrl
	t.signature = Signature
	t.chans = make(map[uint64]chan response.TcaplusResponse)
	t.client = tcaplus.NewClient()

	if err := t.client.SetLogCfg(ConfigPath); err != nil {
		log.Warn("db set log failed %v", err)
	}

	err := t.client.Dial(AppId, []uint32{ZoneId}, DirUrl, Signature, 60)
	if err != nil {
		log.Error("db connect failed %v", err)
		return nil, errors.Append(err, "db connect failed")
	}

	go t.coRecvResponse()

	return t, nil
}

func (t *TcaplusApi) AccessInfo() AccessInfomation {
	info := AccessInfomation{}
	t.mu.Lock()
	info.Count = t.accessInfo.Count
	info.StartTime = t.accessInfo.StartTime
	t.accessInfo.Count = 0
	t.accessInfo.StartTime = time.Now()
	info.EndTime = time.Now()
	t.mu.Unlock()

	return info
}

func (t *TcaplusApi) coRecvResponse() {
	for {
		rsp, err := t.client.RecvResponse()
		if err != nil {
			log.Error("db client rec %v", err)
			//return nil, err
		} else if rsp == nil {
			time.Sleep(time.Microsecond * 100)
		} else {
			log.Trace("tcaplus recv resp %v", rsp)
			sid := rsp.GetAsyncId()
			t.mu.Lock()
			if ch, ok := t.chans[sid]; ok {
				ch <- rsp
			}
			t.mu.Unlock()
			//return rsp, nil
		}
	}
}

func (t *TcaplusApi) recvResponse() (response.TcaplusResponse, error) {
	timeOut := 1
	for {
		rsp, err := t.client.RecvResponse()
		if err != nil {
			return nil, err
		} else if timeOut >= RECV_TIMEOUT*1000000 {
			log.Warn("recv response timeout")
			return nil, errors.New("get response time out")
		} else if rsp == nil {
			timeOut++
			time.Sleep(time.Microsecond * 1)
		} else {
			return rsp, nil
		}
	}
}

func (t *TcaplusApi) GenReq(tableName string, cmd int) (request.TcaplusRequest, uint64, error) {
	//log.Warn("NewRequest...")
	req, err := t.client.NewRequest(t.zoneId, tableName, cmd)
	if err != nil {
		log.Warn("NewRequest failed %v", err)
		return nil, 0, errors.Append(err, "NewRequest failed")
	}
	//log.Warn("%v NewRequest finish", cmd)
	//设置异步请求ID，异步请求通过ID让响应和请求对应起来
	//rand.Seed(time.Now().UnixNano() + int64(len(tableName)))
	//log.Warn("Request to tcaplus...")
	t.mu.Lock()
	//log.Warn("Lock()...")
	t.tsid++
	sid := t.tsid
	//sid := uint64(rand.Uint32())
	if _, ok := t.chans[sid]; ok {
		//log.Warn("%v has exist", sid)
		//sid = uint64(rand.Uint32())
		//log.Warn("new sid is:%v", sid)
		log.Error("Sid:%v has exist", sid)
		t.mu.Unlock()
		return nil, 0, fmt.Errorf("sid has exist")
	}
	t.accessInfo.Count++
	t.count++
	//log.Warn("Request to tcaplus...:%v", t.count)
	t.mu.Unlock()
	//log.Warn("UnLock")
	req.SetAsyncId(sid)
	req.SetMultiResponseFlag(1)
	return req, sid, nil
}

func (t *TcaplusApi) GenRspChan(sid uint64) chan response.TcaplusResponse {
	rspChan := make(chan response.TcaplusResponse, 5)
	t.mu.Lock()
	t.chans[sid] = rspChan
	t.mu.Unlock()
	return rspChan
}

func (t *TcaplusApi) DelRspChan(sid uint64) {
	t.mu.Lock()
	if _, ok := t.chans[sid]; ok {
		close(t.chans[sid])
		delete(t.chans, sid)
	}
	t.mu.Unlock()
}

func (t *TcaplusApi) SendAndRecv(tableName string, cmd int, data record.TdrTableSt, index int32) (*record.Record, error) {
	req, sid, err := t.GenReq(tableName, cmd)
	if err != nil {
		log.Warn("NewRequest failed %v", err)
		return nil, errors.Append(err, "NewRequest failed")
	}

	//log.Warn("Add Record...")
	//为request添加一条记录，（index只有在list表中支持，generic不校验）
	rec, err := req.AddRecord(index)
	if err != nil {
		log.Warn("%v AddRecord failed %v", cmd, err)
		return nil, errors.Append(err, "%v AddRecord failed", cmd)
	}
	log.Trace("%v AddRecord finish", cmd)
	//将tdr的数据设置到请求的记录中
	if err := rec.SetData(data); err != nil {
		log.Warn("%v SetData failed %v", cmd, err)
		return nil, errors.Append(err, "%v SetData failed", cmd)
	}
	//log.Warn("Add Record finish")
	//发送请求
	if err := t.client.SendRequest(req); err != nil {
		log.Warn("%v SendRequest failed %v", cmd, err)
		return nil, errors.Append(err, "%v SendRequest failed", cmd)
	}

	// 创建接收通道
	rspChan := t.GenRspChan(sid)
	defer t.DelRspChan(sid)

	//接收响应
	var rspRecord *record.Record = nil
	for {
		timeOutChan := time.After(RECV_TIMEOUT * time.Second)
		var rsp response.TcaplusResponse
		select {
		case <-timeOutChan:
			log.Warn("get response time out at table:%v|cmd:%v", tableName, cmd)
			return nil, errors.New("get response time out at table:%v", tableName)
		case rsp = <-rspChan:
		}

		tcapluserr := rsp.GetResult()
		if tcapluserr != 0 {
			//目前系统再插入key之前都会先检查key是否存在，
			//如果key不存在，测试检查的结果将出错，错误码是261，因此，对错误码261不做处理
			if tcapluserr != 261 {
				log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
				return nil, errors.New("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
			}
			//log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
		}
		if rsp.GetRecordCount() == 0 {
			break
		}
		if rsp.HaveMoreResPkgs() != 0 || rsp.GetRecordCount() > 1 {
			log.Error("response has a lot of records %v:%v", rsp.HaveMoreResPkgs(), rsp.GetRecordCount())
		}
		for i := 0; i < rsp.GetRecordCount(); i++ {
			rspRecord, err = rsp.FetchRecord()
			if err != nil {
				log.Warn("FetchRecord failed %v|record count %v", err, rsp.GetRecordCount())
				return nil, errors.Append(err, "%v FetchRecord err", cmd)
			}
		}
		if rspRecord != nil {
			break
		}
	}
	return rspRecord, nil
}

func (t *TcaplusApi) RecvBatchRsp(sid uint64) ([]*record.Record, error) {

	// 创建接收通道
	rspChan := t.GenRspChan(sid)
	defer t.DelRspChan(sid)

	var rspRecord []*record.Record = make([]*record.Record, 0)
	var total int = 0
	//log.Warn("Receiveing BatchRsp from tcaplus...")
	for {
		// 等待回包
		timeOutChan := time.After(RECV_TIMEOUT * time.Second)
		var rsp response.TcaplusResponse
		//log.Info("RecvBatchRsp... select...")
		select {
		case <-timeOutChan:
			log.Warn("get response time out")
			return nil, errors.New("get resposne time out")
		case rsp = <-rspChan:
		}
		log.Trace("recv response")

		// 判断回包是否正确
		tcapluserr := rsp.GetResult()
		if tcapluserr != 0 {
			if tcapluserr != 261 {
				log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
				return nil, errors.New("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
			}
			log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
		}

		// response中带有获取的记录
		total += rsp.GetRecordCount()
		log.Trace("response success record count %d, total:%d", rsp.GetRecordCount(), total)

		// 获取回包数据
		idx_max := rsp.GetRecordCount()
		for i := 0; i < idx_max; i++ {
			record, err := rsp.FetchRecord()
			if err != nil {
				log.Warn("FetchRecord failed %s\n", err.Error())
				return nil, errors.Append(err, "FetchRecord err")
			}
			rspRecord = append(rspRecord, record)
		}

		// 没有后续的数据，则退出
		if rsp.HaveMoreResPkgs() == 0 {
			break
		}
	}

	return rspRecord, nil
}

func (t *TcaplusApi) PartSendAndRecv(tableName string, cmd int, data record.TdrTableSt, index string) ([]*record.Record, error) {
	//log.Warn("Start Generate Req(%v)", tableName)
	req, sid, err := t.GenReq(tableName, cmd)
	if err != nil {
		log.Warn("NewRequest failed %v", err)
		return nil, errors.Append(err, "NewRequest failed")
	}
	//log.Warn("Start Generate Req(%v) Done", tableName)
	//为request添加一条记录，（index只有在list表中支持，generic不校验）
	//log.Warn("AddRecord....")
	rec, err := req.AddRecord(0)
	if err != nil {
		log.Warn("%v AddRecord failed %v", cmd, err)
		return nil, errors.Append(err, "%v AddRecord failed", cmd)
	}
	//log.Warn("%v AddRecord finish", cmd)

	if err := rec.SetDataWithIndexAndField(data, nil, index); err != nil {
		log.Warn("%v SetDataWithIndexAndField failed %v", cmd, err)
		return nil, errors.Append(err, "%v SetDataWithIndexAndField failed", cmd)
	}

	//发送请求
	//log.Warn("Start PartSendAndRecv...>>>SendRequest...")
	if err := t.client.SendRequest(req); err != nil {
		log.Warn("%v SendRequest failed %v", cmd, err)
		return nil, errors.Append(err, "%v SendRequest failed", cmd)
	}
	//log.Warn("%v send finish", cmd)

	return t.RecvBatchRsp(sid)
}

func (t *TcaplusApi) BatchSendAndRecv(tableName string, cmd int, data record.TdrTableSt, index string) ([]*record.Record, error) {
	req, sid, err := t.GenReq(tableName, cmd)
	if err != nil {
		log.Warn("NewRequest failed %v", err)
		return nil, errors.Append(err, "NewRequest failed")
	}

	//为request添加一条记录，（index只有在list表中支持，generic不校验）
	rec, err := req.AddRecord(0)
	if err != nil {
		log.Warn("%v AddRecord failed %v", cmd, err)
		return nil, errors.Append(err, "%v AddRecord failed", cmd)
	}
	log.Trace("%v AddRecord finish", cmd)

	if err := rec.SetData(data); err != nil {
		log.Warn("%v SetData failed %v", cmd, err)
		return nil, errors.Append(err, "%v SetData failed", cmd)
	}

	//发送请求
	if err := t.client.SendRequest(req); err != nil {
		log.Warn("%v SendRequest failed %v", cmd, err)
		return nil, errors.Append(err, "%v SendRequest failed", cmd)
	}
	log.Trace("%v send finish", cmd)

	return t.RecvBatchRsp(sid)
}

func (t *TcaplusApi) GetTableRecordCount(tableName string) (int, error) {
	req, sid, err := t.GenReq(tableName, c.TcaplusApiGetTableRecordCountReq)
	if err != nil {
		log.Warn("NewRequest failed %v", err)
		return 0, errors.Append(err, "NewRequest failed")
	}

	//发送请求
	if err := t.client.SendRequest(req); err != nil {
		log.Warn("SendRequest failed %v", err)
		return 0, errors.Append(err, "SendRequest failed")
	}
	log.Trace("send finish")

	// 创建接收通道
	rspChan := t.GenRspChan(sid)
	defer t.DelRspChan(sid)

	//接收响应
	timeOutChan := time.After(RECV_TIMEOUT * time.Second)
	var rsp response.TcaplusResponse
	select {
	case <-timeOutChan:
		log.Warn("get response time out")
		return 0, errors.New("get response time out")
	case rsp = <-rspChan:
	}

	tcapluserr := rsp.GetResult()
	if tcapluserr != 0 {
		if tcapluserr != 261 {
			log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
			return 0, errors.New("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
		}
		log.Warn("response ret errCode: %d, errMsg: %s", tcapluserr, terror.GetErrMsg(tcapluserr))
	}
	return rsp.GetTableRecordCount(), nil
}
