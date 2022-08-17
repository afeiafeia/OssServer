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
)

//当有dataHandle或者dataSaveSvr断开连接时，会调用此函数
func (dispatch *Dispatch) ClearSession(ctx context.Context, session *base.Session) error {

	dispatch.mu.Lock()
	for index, client := range dispatch.dataHandleClients {
		if client == session {
			//说明是dataHandle断开连接了
			delete(dispatch.dataHandleClients, index)
			dispatch.mu.Unlock()
			return nil
		}
	}

	for id, client := range dispatch.nodeSession {
		if client == session {
			//说明是dataSaveSvr断开连接了
			log.Debug("DataSaveSvr session:%v is down", id)
			delete(dispatch.nodeSession, id)
			//向dataHandlef发送同步节点的请求
			go dispatch.DisableNode(id)
		}
	}

	dispatch.mu.Unlock()

	return nil
}

//向dataHandle发送节点下线通知，dataHandle将其记录的该节点状态设置为下线
func (dispatch *Dispatch) DisableNode(nodeId uint64) error {
	req := &app.NodeDisable{
		Id: nodeId,
	}
	//发送请求
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
	header.SetCmd("DisableNode")
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		return fmt.Errorf("Marshal node info header to add failed: %v", err)
	}
	//发送给连接的dataHandle
	for _, s := range dispatch.dataHandleClients {
		err := s.Write(headerBytes, payloadBytes)
		if err != nil {
			log.Warn("send node info to datahandle: %v failed: %v", *s, err)
			continue
		}
		break
	}

	return nil

}
