package dispatch

import (
	"context"
	"oss/app"
	"oss/codec"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"reflect"
)

//在经过sendAddNodeInfo向dataHandle发送AddNodeInfo请求后，将收到回复，此函数处理回复
func (dispatch *Dispatch) BucketFolderDelete(ctx context.Context, req *app.BucketFolderReq) error {

	nodeId := req.NodeId
	dispatch.mu.Lock()
	nodeSession := dispatch.nodeSession
	dispatch.mu.Unlock()

	session, ok := nodeSession[nodeId]
	if !ok {
		return nil
	}

	h := ctx.Value(base.HEADER)
	header := h.(codec.SimpleHeader)

	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding bucketInfo: %v failed: %v", *req, err)
		return nil
	}
	header.SetCmd("DeleteBucketFolder")
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(&header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
	}
	errW := session.Write(headerBytes, payloadBytes)
	if errW != nil {
		//fmt.Printf("failed to write req msg with MsgId:%v ,err is:%v\n", header.GetMsgId(), errW)
		log.Error("failed to write req msg,err is:%v\n", errW)
		log.Error("fail to delete bucket %v", *req)
		return nil
	}
	return nil
}
