package influxdb

import (
	"fmt"
	"oss/lib/base"
	"oss/lib/log"
	"strings"
	"time"
)

const (
	rspMeasurement = "OssResponse"
	Result         = "requestResult"
)

type ResponseInfo struct {
	StartTime int64 //第一个请求的时间
	EndTime   int64 //最后一个请求的时间

	Count     int64 //请求的总数
	ByteCount int64 //字节数

	SuccessCount int64 //成功的请求数量
	FailCount    int64 //失败的请求数量
}

type ClientResponse struct {
	Ret        int    `json:"ret"`        //操作执行的状态
	UserId     string `json:"userId"`     //发出此操作的用户
	Cmd        string `json:"cmd"`        //执行的操作
	BucketName string `json:"bucketName"` //所操作的bucket
	ObjectName string `json:"objectName"` //操作的object
	Service    string `json:"service"`    //对此请求进行处理的服务
	//field
	ByteCount int `json:"byteCount"` //字节数(流量)
}

func (result *ClientResponse) InformationString() string {
	var state string
	if result.Ret == base.RET_OK {
		state = "OK"
	} else {
		state = "FAIL"
	}
	str := fmt.Sprintf("State:%v|UserId:%v|Cmd:%v|BucketName:%v|ObjectName:%v|Service:%v|ByteCount:%v", state, result.UserId, result.Cmd, result.BucketName, result.ObjectName, result.Service, result.ByteCount)
	return str
}

//上报服务端对于客户端请求的处理结果
func (client *InfluxdbApi) UploadClientResponse(rsp *ClientResponse) error {

	//版本信息修改为：version + "::" + gameId + "::" + clientId的形式
	tags := make(map[string]string, 0)
	tags[User] = rsp.UserId
	tags[CMD] = rsp.Cmd
	tags[BucketName] = rsp.BucketName
	tags[ObjectName] = rsp.ObjectName
	tags[Service] = rsp.Service
	if rsp.Ret == base.RET_OK {
		tags[Result] = "OK"
	} else {
		tags[Result] = "ERROR"
	}
	//汇总数据
	measurement := rspMeasurement
	fields := make(map[string]interface{}, 0)
	fields[ByteCount] = rsp.ByteCount
	fields[Count] = 1
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		return fmt.Errorf("failed to save summary data:%v\n", err)
	}

	return nil
}

//获取服务指定时间段内响应客户端的数据信息，目前不再使用
func (client *InfluxdbApi) ResponseInfo(startTime, endTime time.Time) (ResponseInfo, error) {

	info := ResponseInfo{}
	//measurement是OssResponse
	//tag项cmd分别是UploadMetadata、UploadSliceData、ObjectQuery、ObjectDelete

	CMDSet := make([]string, 0)
	CMDSet = append(CMDSet, "UploadMetadata")
	CMDSet = append(CMDSet, "UploadSliceData")
	CMDSet = append(CMDSet, "ObjectQuery")
	CMDSet = append(CMDSet, "ObjectDelete")
	measurement := rspMeasurement
	tag := make(map[string]string)
	fileds := make([]string, 0)

	count := (int64)(0)
	success := (int64)(0)
	fail := (int64)(0)
	min := endTime.UnixMilli()
	max := startTime.UnixMilli()
	byteCount := 0
	for _, cmd := range CMDSet {
		tag[CMD] = cmd
		recordSet, err := client.QueryDataByParmsMap(measurement, tag, fileds, startTime, endTime)
		if err != nil {
			log.Error("QueryDataByParmsMap by |Measurement:%v|tag:%v|failed:%v", measurement, tag, err)
			continue
		}
		for recordSet.Next() {
			record := recordSet.Record()
			state := record.ValueByKey(Result).(string)
			if strings.Compare(state, "OK") == 0 {
				success++
			} else {
				fail++
			}
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
	info.SuccessCount = success
	info.FailCount = fail

	return info, nil
}

//获取所有回复信息(目前暂未使用，后期可用于查看用户请求的详细信息)
func (client *InfluxdbApi) GetAllServerResponse() ([]ClientResponse, error) {

	//汇总数据
	measurement := rspMeasurement
	tags := make(map[string]string, 0)
	fields := make([]string, 0)
	endTime := time.Now()
	startTime := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour()-2, 0, 0, 0, time.Local)
	recordSet, err := client.QueryDataByParmsMap(measurement, tags, fields, startTime, endTime)
	if err != nil {
		log.Error("Get %v's all request failed:%v", measurement, err)
		return nil, fmt.Errorf("failed to save request data:%v\n", err)
	}

	result := make([]ClientResponse, 0)
	for recordSet.Next() {
		record := recordSet.Record()

		curRes := ClientResponse{}
		state := record.ValueByKey(Result).(string)
		if strings.Compare(state, "OK") == 0 {
			curRes.Ret = 1
		} else {
			curRes.Ret = -1
		}
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
