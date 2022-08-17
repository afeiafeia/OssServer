package dataHandle

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

	//租户数量
	userMeasurement = "UserMeasurement"
	UserCount       = "UserCount"

	//tcaplus采样时间，每10秒统计一下这10秒内对tcaplus的访问次数
	TcaplusSimpleTime = 3
)

var TcaplusAccessInfoSimple *time.Timer

func (dataHandler *DataHandle) uploadObjectDeleteRequest(ctx context.Context, req *app.ObjectReq) error {

	//return nil
	//获取cmd
	//cmd := "DeleteObject"
	//从上下文中获取头部
	log.Debug("start uploadObjectDeleteRequest")
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	request := &influxdb.ClientRequest{
		Cmd:       cmd,
		ByteCount: header.GetPayloadLength() + HeaderSize,
		Service:   "dataHandle",
	}

	//从req中获取详细信息：用户、bucket、object
	request.UserId = req.UserId
	request.BucketName = req.BucketName
	request.ObjectName = req.ObjName
	//上报请求
	log.Debug("Upload information to influxdb:%v", request)
	err := dataHandler.GetInfluxdbClient().UploadClientRequest(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return nil
}

func (dataHandler *DataHandle) uploadObjectDeleteResult(ctx context.Context, req *app.ObjectReq, Ret int) error {

	return nil
	//获取cmd
	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	b, _ := json.Marshal(req)
	rspSize := len(b)
	//cmd := "DeleteObject"
	request := &influxdb.ClientResponse{
		Ret:       Ret,
		Cmd:       cmd,
		ByteCount: rspSize + HeaderSize,
		Service:   "dataHandle",
	}

	//从req中获取详细信息：用户、bucket、object
	request.UserId = req.UserId
	request.BucketName = req.BucketName
	request.ObjectName = req.ObjName
	//上报请求
	err := dataHandler.GetInfluxdbClient().UploadClientResponse(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return nil
}

//client发送过来元数据上报请求后，此处上报到influxdb,由底层调用
func (dataHandler *DataHandle) uploadMetadataRequest(ctx context.Context, req *app.ObjectMetadataUploadReq) error {

	//return nil
	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	request := &influxdb.ClientRequest{
		Cmd:       cmd,
		ByteCount: header.GetPayloadLength() + HeaderSize,
		Service:   "dataHandle",
	}

	//从req中获取详细信息：用户、bucket、object
	request.UserId = req.UserId
	request.BucketName = req.BucketName
	request.ObjectName = req.ObjName
	//上报请求
	log.Debug("Upload information to influxdb:%v", request)
	err := dataHandler.GetInfluxdbClient().UploadClientRequest(request)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", request, err)
	}

	return nil
}

//dataHandle根据元数据生成落地方案后，经dispatch转发给client，此处上报至influxdb，由底层直接调用
func (dataHandler *DataHandle) uploadMetadataResult(ctx context.Context, rsp *app.ObjectMetadataUploadRsp) error {

	//return nil
	//从上下文中获取头部
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	//获取cmd
	cmd := header.GetCmd()
	b, _ := json.Marshal(rsp)
	rspSize := len(b)
	requestResult := &influxdb.ClientResponse{
		Cmd:       cmd,
		Ret:       rsp.Ret,
		ByteCount: rspSize + HeaderSize,
		Service:   "dataHandle",
	}

	//从rsp的taskId中解析出userId,bucketName,objectName
	requestResult.UserId, requestResult.BucketName, requestResult.ObjectName = tcaplus_api.DecodeObjectId(rsp.TaskId)

	//上报请求
	//log.Debug("Upload information to influxdb:%v", requestResult)
	err := dataHandler.GetInfluxdbClient().UploadClientResponse(requestResult)
	if err != nil {
		log.Warn("Upload message:%v to influxDB failed:%v", requestResult, err)
	}

	return nil
}

//服务启动后，上报服务中注册的用户数量
func (dataHandler *DataHandle) uploadUserCount() {

	defer func() {
		expire := calUserCountUpdataExpireTime()
		UserCountTimer.Reset(expire)
	}()
	tableName := "tb_user"
	recordCount, err := dataHandler.GetTcaplusClient().GetTableRecordCount(tableName)
	if err != nil {
		log.Error("Get user count failed:%v", err)
	}

	//该measurement中只有一条数据，没有tag,value就是目前的租户数
	//先删除，再插入
	err = dataHandler.GetInfluxdbClient().DeleteMeasurement(userMeasurement)
	if err != nil {
		log.Error("Delete UserMeasurement failed:%v", err)
	}

	var userCount int64
	userCount = (int64)(recordCount)
	tags := make(map[string]string, 0)
	fields := make(map[string]interface{}, 0)
	fields[UserCount] = userCount

	err = dataHandler.GetInfluxdbClient().WriteDataToTsDB(userMeasurement, fields, tags)
	if err != nil {
		log.Error("Updata user count failed:%v", err)
	}
}

//每当创建新用户时，更新一下记录
func (dataHandler *DataHandle) updateUserCount() {

	//先获取原始用户数
	measurement := userMeasurement
	tags := make(map[string]string, 0)
	fields := make([]string, 0)
	endTime := time.Now()
	startTime := time.Date(endTime.Year(), endTime.Month()-2, endTime.Day(), endTime.Hour(), 0, 0, 0, time.Local)
	recordSet, err := dataHandler.GetInfluxdbClient().QueryDataByParmsMap(measurement, tags, fields, startTime, endTime)
	if err != nil {
		log.Error("Get %v's all request failed:%v", measurement, err)
		return
	}

	var userCount int64
	for recordSet.Next() {
		record := recordSet.Record()

		userCount = record.Value().(int64)
	}
	userCount++
	//该measurement中只有一条数据，没有tag,value就是目前的租户数
	//先删除，再插入
	err = dataHandler.GetInfluxdbClient().DeleteMeasurement(userMeasurement)
	if err != nil {
		log.Error("Delete UserMeasurement failed:%v", err)
	}

	tags = make(map[string]string, 0)
	field := make(map[string]interface{}, 0)
	field[UserCount] = userCount

	err = dataHandler.GetInfluxdbClient().WriteDataToTsDB(userMeasurement, field, tags)
	if err != nil {
		log.Error("Updata user count failed:%v", err)
	}
}

//定期上报tcaplus的请求次数
func (dataHandler *DataHandle) UploadTcaplusAccessInfo() {

	bucketInfo := dataHandler.GetTcaplusClient().AccessInfo()
	objectInfo := dataHandler.GetTcaplusClientObject().AccessInfo()

	accessInfo := influxdb.TcaplusAccess{}
	accessInfo.Client = "BucketClient"
	accessInfo.SimpleTime = bucketInfo.EndTime.UnixMilli() - bucketInfo.StartTime.UnixMilli()
	accessInfo.AccessCount = bucketInfo.Count
	accessInfo.QPS = 1000 * accessInfo.AccessCount / accessInfo.SimpleTime

	err := dataHandler.GetInfluxdbClient().UploadTcaplusAccessInfo(accessInfo)
	if err != nil {
		log.Error("Upload bucket tcaplus info:%v failed:%v", accessInfo, err)
		return
	}

	accessInfo.Client = "ObjectClient"
	accessInfo.SimpleTime = objectInfo.EndTime.UnixMilli() - objectInfo.StartTime.UnixMilli()
	accessInfo.AccessCount = objectInfo.Count
	accessInfo.QPS = 1000 * accessInfo.AccessCount / accessInfo.SimpleTime

	dataHandler.GetInfluxdbClient().UploadTcaplusAccessInfo(accessInfo)
	if err != nil {
		log.Error("Upload object tcaplus info:%v failed:%v", accessInfo, err)
		return
	}

	expire := TcaplusSimpleTime * time.Second
	TcaplusAccessInfoSimple.Reset(expire)
}
