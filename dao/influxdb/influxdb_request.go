//对于client发送过来的请求的上报
//主要记录：哪个用户，对哪个bucket、哪个object，进行了何种操作
package influxdb

import (
	"fmt"
	"oss/lib/log"
	"time"
)

const (
	reqMeasurement = "OssRequest"
	User           = "User"       //用户
	CMD            = "Method"     //请求类型：上传、下载、删除
	BucketName     = "BucketName" //操作的bucket的名称
	ObjectName     = "ObjectName" //操作的对象名称
	Service        = "Service"    //处理该请求的服务节点：dispatchSvr、dataHandle、dataSaveSvr
	ByteCount      = "ByteCount"  //字节数
	Count          = "Count"      //次数
)

type RequestInfo struct {
	StartTime int64 //第一个请求的时间
	EndTime   int64 //最后一个请求的时间

	Count     int64 //请求的总数
	ByteCount int64 //字节数
}

type ClientRequest struct {
	//tag
	UserId     string //发出此操作的用户
	Cmd        string //执行的操作
	BucketName string //所操作的bucket
	ObjectName string //操作的object
	Service    string //对此请求进行处理的服务
	ByteCount  int    //字节数(流量)
	Count      int    //每一个请求的Count都是1，该项用于grafana统计次数(检索Request的count字段，进行累加，即为总请求数)
}

func (request *ClientRequest) InformationString() string {
	str := fmt.Sprintf("UserId:%v|Cmd:%v|BucketName:%v|ObjectName:%v|Service:%v|ByteCount:%v", request.UserId, request.Cmd, request.BucketName, request.ObjectName, request.Service, request.ByteCount)
	return str
}

//向influxdb上报客户端的一条请求信息
func (client *InfluxdbApi) UploadClientRequest(req *ClientRequest) error {

	tags := make(map[string]string, 0)
	tags[User] = req.UserId
	tags[CMD] = req.Cmd
	tags[BucketName] = req.BucketName
	tags[ObjectName] = req.ObjectName
	tags[Service] = req.Service
	//汇总数据
	measurement := reqMeasurement
	fields := make(map[string]interface{}, 0)
	fields[ByteCount] = req.ByteCount
	fields[Count] = 1
	log.Debug("Write info:%v", req.InformationString())
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		return fmt.Errorf("failed to save request data:%v\n", err)
	}
	return nil
}

//获取所有请求信息(该函数目前暂未使用，后期可用于查看用户请求的详细信息)
func (client *InfluxdbApi) GetAllClientRequest() ([]ClientRequest, error) {

	//汇总数据
	measurement := reqMeasurement
	tags := make(map[string]string, 0)
	fields := make([]string, 0)
	endTime := time.Now()
	startTime := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour()-2, 0, 0, 0, time.Local)
	recordSet, err := client.QueryDataByParmsMap(measurement, tags, fields, startTime, endTime)
	if err != nil {
		log.Error("Get %v's all request failed:%v", measurement, err)
		return nil, fmt.Errorf("failed to save request data:%v\n", err)
	}

	result := make([]ClientRequest, 0)
	for recordSet.Next() {
		record := recordSet.Record()

		curRes := ClientRequest{}
		curRes.UserId = record.ValueByKey(User).(string)
		curRes.Cmd = record.ValueByKey(CMD).(string)
		curRes.BucketName = record.ValueByKey(BucketName).(string)
		curRes.ObjectName = record.ValueByKey(ObjectName).(string)
		curRes.Service = record.ValueByKey(Service).(string)
		bytes := record.Value().(int64)
		curRes.ByteCount = (int)(bytes)

		log.Debug("time is:%v", record.Time())
		result = append(result, curRes)
	}
	return result, nil
}

//获取指定时间段内的请求信息，用于统计QPS，目前使用新方式(该函数目前暂未使用，后期可用于查看用户请求的详细信息)
func (client *InfluxdbApi) RequestInfo(startTime, endTime time.Time) (RequestInfo, error) {

	//
	info := RequestInfo{}
	measurement := reqMeasurement
	CMDSet := make([]string, 0)
	CMDSet = append(CMDSet, "UploadMetadata")
	CMDSet = append(CMDSet, "UploadSliceData")
	CMDSet = append(CMDSet, "ObjectQuery")
	CMDSet = append(CMDSet, "ObjectDelete")

	tag := make(map[string]string)
	fileds := make([]string, 0)

	count := (int64)(0)
	min := endTime.UnixMilli()
	max := startTime.UnixMilli()
	byteCount := 0
	for _, cmd := range CMDSet {
		tag[CMD] = cmd
		recordSet, err := client.QueryDataByParmsMap(measurement, tag, fileds, startTime, endTime)
		if err != nil {
			log.Error("QueryDataByParmsMap by |Measurement:%v|tag:%v|failed:%v", measurement, tag, err)
			continue
			//return 0, 0, fmt.Errorf("QueryDataByParmsMap failed:%v", err)
		}
		for recordSet.Next() {
			record := recordSet.Record()

			curByteCount := record.Value().(int64)
			bytes := (int)(curByteCount)
			count++
			byteCount += bytes
			//查找时间的最大值和最小值
			curTime := record.Time().UnixMilli()
			if curTime < min {
				min = curTime
			}
			if curTime > max {
				max = curTime
			}
		}
	}

	info.StartTime = min
	info.EndTime = max
	info.Count = count
	info.ByteCount = int64(byteCount)

	return info, nil
}
