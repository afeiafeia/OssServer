package dispatch

import (
	"context"
	"encoding/json"
	"oss/app"
	"oss/codec"
	"oss/dao/influxdb"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"time"
)

const (
	HeaderSize = 108

	ClientSimplePeriodic = 2

	ClientReqRspInfo = "ClientReqRspInfo"
	ReqCount         = "ReqCount"
	RspCount         = "RspCount"
	RspSuccessCount  = "RspSuccessCount"
	SuccessRate      = "SuccessRate"
)

var ClientInfoTimer *time.Timer

//client发送过来元数据上报请求后，此处上报到influxdb,由底层调用
func (dispatch *Dispatch) UploadMetadataRequest(ctx context.Context, req *app.ObjectMetadataUploadReq) error {

	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	request := &influxdb.ClientRequest{
		Cmd:       cmd,
		ByteCount: header.GetPayloadLength() + HeaderSize,
		Service:   "dispatchSvr",
	}

	//从req中获取详细信息：用户、bucket、object
	request.UserId = req.UserId
	request.BucketName = req.BucketName
	request.ObjectName = req.ObjName
	//上报请求
	err := dispatch.influxdb_client.UploadClientRequest(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return nil
}

//dataHandle根据元数据生成落地方案后，经dispatch转发给client，此处上报至influxdb，由底层直接调用
func (dispatch *Dispatch) UploadMetadataResult(ctx context.Context, rsp *app.ObjectMetadataUploadRsp) error {

	//return nil
	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	b, _ := json.Marshal(rsp)
	rspSize := len(b)
	requestResult := &influxdb.ClientResponse{
		Cmd:       cmd,
		ByteCount: rspSize + HeaderSize,
		Service:   "dispatchSvr",
	}

	//从rsp的taskId中解析出userId,bucketName,objectName
	requestResult.UserId, requestResult.BucketName, requestResult.ObjectName = tcaplus_api.DecodeObjectId(rsp.TaskId)

	//上报请求
	err := dispatch.influxdb_client.UploadClientResponse(requestResult)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", requestResult, err)
	}

	return nil
}

//dispatch收到client发送的对象分片时，上报至influxdb,在逻辑层UploadSliceData开启协程进行上报
func (dispatch *Dispatch) UploadObjectSliceRequest(ctx context.Context) error {
	//return nil
	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	request := &influxdb.ClientRequest{
		Cmd:       cmd,
		ByteCount: header.GetPayloadLength() + HeaderSize,
		Service:   "dispatchSvr",
	}

	//从头部的的taskId中解析出userId,bucketName,objectName
	request.UserId, request.BucketName, request.ObjectName = tcaplus_api.DecodeObjectId(header.GetTaskId())
	//上报请求
	log.Debug("Upload ObjectSliceRequest information to influxdb:%v", request)
	err := dispatch.influxdb_client.UploadClientRequest(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return nil
}

//上报分片存储的最终结果至influxdb，在逻辑层UploadSliceData收到client发送过来的对象分片后，开启协程进行上报
func (dispatch *Dispatch) UploadObjectSliceResult(ctx context.Context, rsp *app.ObjectUploadRsp) {
	//return
	//cmd
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	b, _ := json.Marshal(rsp)
	rspSize := len(b)
	result := &influxdb.ClientResponse{
		Cmd:       cmd,
		Ret:       rsp.Ret,
		ByteCount: rspSize + HeaderSize,
		Service:   "dispatchSvr",
	}
	result.UserId, result.BucketName, result.ObjectName = tcaplus_api.DecodeObjectId(rsp.ObjectId)
	//上报请求
	log.Debug("Upload ObjectSliceResult information to influxdb:%v", result)
	err := dispatch.influxdb_client.UploadClientResponse(result)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", result, err)
	}

	return
}

//上报对象查询请求，不经底层，在逻辑层ObjectQuery开启协程进行处理
//如果在底层处理，req需要解析处理，而在ObjectQuery需要再解析一次
func (dispatch *Dispatch) UploadObjectQueryRequest(ctx context.Context, req *app.ObjectReq) {
	//return
	//cmd,对于查询请求，不统计请求报文的长度，在回复时，统计对象实体的长度
	//cmd := "ObjectQuery"
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	request := &influxdb.ClientRequest{
		Cmd:       cmd,
		ByteCount: header.GetPayloadLength() + HeaderSize,
		Service:   "dispatchSvr",
	}
	//从req中获取详细信息：用户、bucket、object
	request.UserId = req.UserId
	request.BucketName = req.BucketName
	request.ObjectName = req.ObjName
	//上报请求
	log.Debug("Upload ObjectQueryResult information to influxdb:%v", request)
	err := dispatch.influxdb_client.UploadClientRequest(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return
}

//在逻辑层开启协程进行上报
func (dispatch *Dispatch) UploadObjectQueryResult(objId string, res, length int) error {
	return nil
	//cmd,对于查询请求，不统计请求报文的长度，在回复时，统计对象实体的长度
	cmd := "ObjectQuery"
	result := &influxdb.ClientResponse{
		Ret:       res,
		Cmd:       cmd,
		ByteCount: length + HeaderSize,
		Service:   "dispatchSvr",
	}
	//从req中获取详细信息：用户、bucket、object
	result.UserId, result.BucketName, result.ObjectName = tcaplus_api.DecodeObjectId(objId)
	//上报请求
	log.Debug("Upload ObjectQueryResult information to influxdb:%v", result)
	err := dispatch.influxdb_client.UploadClientResponse(result)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", result, err)
	}

	return nil
}

func (dispatch *Dispatch) uploadClientReqRspInfo() {

	defer func() {
		expire := ClientSimplePeriodic * time.Second
		ClientInfoTimer.Reset(expire)
	}()
	now := time.Now()
	dispatch.mu.Lock()
	reqCount := dispatch.clientReqCount
	rspCount := dispatch.clientRspCount
	rspSuccessCount := dispatch.clientRspSuccessCount
	//重新统计
	dispatch.clientReqCount = 0
	dispatch.clientRspCount = 0
	dispatch.clientRspSuccessCount = 0
	startTime := dispatch.SimpleClientInfoTime
	dispatch.SimpleClientInfoTime = now
	dispatch.mu.Unlock()

	//处理客户端请求的QPS
	t := now.UnixMilli() - startTime.UnixMilli()
	var QPS int64
	if t != 0 {
		QPS = 1000 * rspCount / t
	}

	var successRate float32
	if rspCount != 0 {
		successRate = (float32)(rspSuccessCount) / (float32)(rspCount)
		if successRate > 1.0 {
			//由于浮点数精度误差，该值可能大于1.0，此处判断，如果大于1.0,直接置为1.0
			successRate = 1.0
		}
	}
	info := influxdb.ClientReqRspInfo{}
	info.QPS = QPS
	info.ReqCount = reqCount
	//当rspCount为0时，influxdb中不会上报成功率SuccessRate
	info.RspCount = rspCount
	info.RspSuccessCount = rspSuccessCount
	info.SuccessRate = successRate

	dispatch.GetInfluxdbClient().UploadClientReqRspInfo(&info)

}
