package influxdb

import (
	"oss/lib/base"
	"oss/lib/log"
	"time"
)

const (
	BenchInfoMeasurement = "OssBenchInfo"
	OssClientReqRspInfo  = "OssClientReqRspInfo"
	Time                 = "Time"
	ReqCount             = "ReqCount"
	RspCount             = "RspCount"
	RspSuccess           = "RspSuccess"
	RspFail              = "RspFail"
	SuccessRatio         = "SuccessRatio"
	QPS                  = "QPS"
	ByteFlow             = "ByteFlow"

	RspSuccessCount = "RspSuccessCount"
	SuccessRate     = "SuccessRate"
)

type ServerQPSInfo struct {
	Server   string //所属服务
	QPS      int64  //QPS：每秒钟处理的请求数量
	ByteFlow int64  //每秒钟处理的字节数量
}

type BenchInfo struct {
	Service      string  //所属服务
	Time         int64   //统计的总时间：毫秒为单位
	ReqCount     int64   //总请求数
	RspCount     int64   //总响应数
	RspSuccess   int64   //响应为成功的响应数
	RspFail      int64   //响应为失败的响应数
	SuccessRatio float32 //成功率
	QPS          int64   //QPS每秒钟处理的请求数量
	ByteFlow     int64   //每秒钟处理的字节数量
}

type ClientReqRspInfo struct {
	QPS             int64
	ReqCount        int64
	RspCount        int64
	RspSuccessCount int64
	SuccessRate     float32
}

//上报QPS、流量的信息(暂不使用)
func (client *InfluxdbApi) GetClientRequestQPS(periodic int) {

	info := BenchInfo{}
	endTime := time.Now()
	startTime := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(), endTime.Minute(), endTime.Second()-periodic, endTime.Nanosecond(), time.Local)

	reqInfo, err := client.RequestInfo(startTime, endTime)
	if err != nil {
		log.Error("Get request info failed:%v", err)
		return
	}
	//log.Info("Request info is:%v", reqInfo)
	rspInfo, err := client.ResponseInfo(startTime, endTime)
	if err != nil {
		log.Error("Get request info failed:%v", err)
		return
	}
	//log.Info("Response info is:%v", rspInfo)
	//return
	//第一个请求到达的时间
	minTime := reqInfo.StartTime
	//最后一个请求处理完的时间
	maxTime := rspInfo.EndTime

	//二者的时间差就是处理时间
	if minTime < maxTime {
		info.Time = maxTime - minTime
		//客户端发送过来的请求数量
		info.ReqCount = reqInfo.Count
		//服务端向客户端回复的回复数量
		info.RspCount = rspInfo.Count

		//回复的报文中，表示请求处理成功的回复的数量
		info.RspSuccess = rspInfo.SuccessCount
		//回复的报文中，表示请求处理失败的回复的数量
		info.RspFail = rspInfo.SuccessCount

		var count float32
		count = float32(info.ReqCount)
		//成功率
		info.SuccessRatio = (float32)(info.RspSuccess) / count
		if info.SuccessRatio > 1.0 {
			//由于浮点数精度误差，该值可能大于1.0，此处判断，如果大于1.0,直接置为1.0
			info.SuccessRatio = 1.0
		}
		//统计时间内，请求与响应的总数据量
		byteCount := reqInfo.ByteCount + rspInfo.ByteCount

		//QPS：每秒钟响应请求数
		info.QPS = 1000 * info.ReqCount / info.Time
		//每秒钟处理的数据量：字节为单位
		info.ByteFlow = 1000 * byteCount / info.Time
	}

	//将BenchInfo上报至influxdb
	client.UploadBenchInfo(&info)
	log.Info("QPS:%v|	ByteFlow:%v", QPS, ByteFlow)
}

//统计上报服务全链路的QPS、流量信息(目前使用该方式)
func (client *InfluxdbApi) CalServerQPS(QPSInfo base.ReqRspInfo, Server string) {

	info := ServerQPSInfo{
		Server: Server,
	}
	//统计的起始时刻
	minTime := QPSInfo.StartTime.UnixMilli()
	//统计的结束时刻
	maxTime := time.Now().UnixMilli()
	//总统计时间
	t := maxTime - minTime
	//统计时间内，请求与响应的总数据量
	byteCount := QPSInfo.ReqByteCount + QPSInfo.RspByteCount
	//QPS：每秒钟响应请求数
	info.QPS = 1000 * QPSInfo.RspCount / t
	//每秒钟处理的数据量：字节为单位
	info.ByteFlow = 1000 * byteCount / t

	//将BenchInfo上报至influxdb
	client.UploadServerQPSInfo(&info)
	//log.Info("QPS:%v|	ByteFlow:%v", QPS, ByteFlow)
}

//向influxdb上报QPS、流量信息
func (client *InfluxdbApi) UploadServerQPSInfo(info *ServerQPSInfo) {

	measurement := BenchInfoMeasurement
	tags := make(map[string]string, 0)
	fieldName := info.Server + QPS
	tags["Server"] = fieldName
	fields := make(map[string]interface{}, 0)
	fields[QPS] = info.QPS
	fields[ByteFlow] = info.ByteFlow
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		log.Error("Write bench info:%v to influxdb failed:%v", info, err)
		return
	}
}

func (client *InfluxdbApi) UploadBenchInfo(info *BenchInfo) {
	//版本信息修改为：version + "::" + gameId + "::" + clientId的形式

	measurement := BenchInfoMeasurement
	tags := make(map[string]string, 0)
	fields := make(map[string]interface{}, 0)
	fields[Time] = info.Time
	fields[ReqCount] = info.ReqCount
	fields[RspCount] = info.RspCount
	fields[RspSuccess] = info.RspSuccess
	fields[RspFail] = info.RspFail
	fields[SuccessRatio] = info.SuccessRatio
	fields[QPS] = info.QPS
	fields[ByteFlow] = info.ByteFlow
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		log.Error("Write bench info:%v to influxdb failed:%v", info, err)
		return
	}
}

//上报整个服务相对于客户端的QPS信息(不是全链路，全链路的QPS信息由CalServerQPS上报)
func (client *InfluxdbApi) UploadClientReqRspInfo(info *ClientReqRspInfo) {
	measurement := OssClientReqRspInfo
	tags := make(map[string]string, 0)
	fields := make(map[string]interface{}, 0)
	fields[QPS] = info.QPS
	fields[ReqCount] = info.ReqCount
	fields[RspCount] = info.RspCount
	fields[RspSuccessCount] = info.RspSuccessCount
	if info.RspCount > 0 {
		fields[SuccessRate] = info.SuccessRate
	}
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		log.Error("Write bench info :%v to influxdb failed:%v", *info, err)
		return
	}

}
