package dispatch

import (
	"context"
	"fmt"
	"oss/app"
	"oss/codec"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"reflect"
	"strconv"
	"time"
)

//在经过sendAddNodeInfo向dataHandle发送AddNodeInfo请求后，将收到回复，此函数处理回复
func (dispatch *Dispatch) AddNodeInfo(ctx context.Context, rsp *app.NodeBriefInfoRsp) error {

	log.Debug("Receive sync rsp:%v", *rsp)
	dispatch.mu.Lock()
	if dispatch.syncNodeInfoChan == nil {
		dispatch.mu.Unlock()
		log.Warn("Node info:%v is coming after time out", *rsp)
		return nil
	}
	log.Debug("Get NodeBriefInfoRsp:%v", *rsp)
	if rsp.Ret == base.RET_ERROR {
		dispatch.mu.Unlock()
		log.Error("AddNodeInfo failed:%v", rsp.ErrorMsg)
		dispatch.syncNodeInfoChan <- (-1)
		return nil
	}

	dispatch.mu.Unlock()
	if len(rsp.NodeBriefSet) == 1 {
		id := (int64)(rsp.NodeBriefSet[0].Id)
		log.Debug("Nodeid is :%v", id)
		dispatch.syncNodeInfoChan <- id
	} else {
		dispatch.syncNodeInfoChan <- 1
	}
	for _, brief := range rsp.NodeBriefSet {
		if brief.Ret != base.RET_OK {
			//当前节点记录到tcaplus中失败
			log.Warn("Record nodeSvr:%v's information to tcaplus fail", brief)
			continue
		}
		key := brief.Ip + ":" + strconv.Itoa((int)(brief.Port))
		session := dispatch.base.GetSession(key)
		log.Debug("Get session:%v by index: %v", session, key)
		dispatch.mu.Lock()
		dispatch.nodeSession[brief.Id] = session
		dispatch.mu.Unlock()

	}

	log.Debug("Sync node info success!")
	log.Debug("Dispatch's nodeSession is: %v", dispatch.nodeSession)
	return nil
}

//存储节点nodeSvr主动接入请求：服务启动后扩展节点
func (dispatch *Dispatch) ConnectToOssService(ctx context.Context, req *app.NodeInfo, rsp *app.NodeBrief) error {

	key := req.Ip + ":" + strconv.Itoa((int)(req.Port))
	s := ctx.Value("session")
	session := s.(*base.Session)
	session.ChangeIndex(key)

	log.Debug("Sync ndoeInfo with datahandle...%v", *req)
	rsp.Ret = base.RET_ERROR

	nodeSet := &app.NodeInfoUpdateReq{}
	nodeSet.Node = make([]app.NodeInfo, 0)
	nodeSet.Node = append(nodeSet.Node, *req)

	dispatch.mu.Lock()
	if dispatch.syncNodeInfoChan != nil {
		log.Warn("Service is Syncing nodeSvr info now,try again after a few seconds")
		dispatch.mu.Unlock()
		return nil
	} else {
		dispatch.syncNodeInfoChan = make(chan int64, 1)
	}
	dispatch.mu.Unlock()
	defer func() {
		dispatch.mu.Lock()
		if dispatch.syncNodeInfoChan != nil {
			close(dispatch.syncNodeInfoChan)
			dispatch.syncNodeInfoChan = nil
		}
		dispatch.mu.Unlock()
	}()

	//发送请求
	//构造头部
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(nodeSet))
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
		log.Error("sync node info:%v time out", *req)
		return fmt.Errorf("sync node info:%v time out", *req)
	case res := <-dispatch.syncNodeInfoChan:
		if res == -1 {
			log.Error("sync info:%v failed", *req)
			return fmt.Errorf("sync info:%v failed", *req)
		} else {
			rsp.Id = (uint64)(res)
		}
	}
	rsp.Ret = base.RET_OK
	return nil
}
