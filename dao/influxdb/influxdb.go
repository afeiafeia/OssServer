package influxdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"oss/lib/log"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/rangechow/errors"
)

type InfluxdbApi struct {

	// influxdb地址,ex:http://localhost:8086
	Addr string
	// influxdb的Org
	Org string
	// 用户的密码
	Token string
	// 访问的DB(bucket)
	Bucket string

	// influxdb client的句柄
	client influxdb2.Client
	// 锁
	mu sync.Mutex
}

func NewInfludbClient(org, token, addr, bucket string) *InfluxdbApi {

	var i *InfluxdbApi = new(InfluxdbApi)

	i.Org = org
	i.Token = token
	i.Addr = addr
	i.Bucket = bucket

	i.client = influxdb2.NewClient(addr, token)

	return i
}

func (i *InfluxdbApi) GetBucket() string {
	return i.Bucket
}

// point参考https://pkg.go.dev/github.com/influxdata/influxdb-client-go/v2@v2.8.1/api/write#Point
func (i *InfluxdbApi) WritePoint(p *write.Point) error {

	// Get Write API client
	writeAPI := i.client.WriteAPIBlocking(i.Org, i.Bucket)
	// wait result
	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		log.Error("write point failed %v", err)
		return err
	}
	return nil
}

// query和params 参考https://docs.influxdata.com/flux/v0.x/stdlib/universe/
func (i *InfluxdbApi) QueryPoints(query string) (*api.QueryTableResult, error) {

	// Get Query API client
	queryAPI := i.client.QueryAPI(i.Org)
	// Get result
	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Error("read point failed %v", err)
		return nil, err
	}

	return result, nil
}

// predicate 参考https://docs.influxdata.com/influxdb/cloud/write-data/delete-data/
func (i *InfluxdbApi) DeletePoints(start, stop time.Time, predicate string) error {

	// Get Delete API client
	deleteAPI := i.client.DeleteAPI()
	// Delete last hour data with tag b = static
	err := deleteAPI.DeleteWithName(context.Background(), i.Org, i.Bucket, start, stop, predicate)
	if err != nil {
		log.Error("delete point failed %v", err)
		return err
	}

	return nil
}

func (i *InfluxdbApi) GetMeasurementPoint(measurement string) *write.Point {
	return influxdb2.NewPointWithMeasurement(measurement)
}

func (i *InfluxdbApi) QueryPointsByParam(measurement, tag, field string) (*api.QueryTableResult, error) {

	//如果没有指定时间，默认时间区间是：两个月前到当前时间
	end := time.Now()
	start := time.Date(end.Year(), end.Month()-2, end.Day(), end.Hour(), end.Minute(), end.Second(), end.Nanosecond(), time.Local)
	return i.QueryDataByParms(measurement, tag, field, start, end)
}

func (i *InfluxdbApi) QueryPointsByParamConcurrence(measurement, tag string, fieldNameArr []string) ([]*api.QueryTableResult, error) {
	index := 0
	space := 60
	var resArr []*api.QueryTableResult
	wg := sync.WaitGroup{}
	channelLen := len(fieldNameArr)/space + 1
	workChannel := make(chan struct{}, 15) // 最多支持30个并发
	errChannel := make(chan error, channelLen)
	resChannel := make(chan *api.QueryTableResult, channelLen)
	for index < len(fieldNameArr) {
		wg.Add(1)
		go func(index int) {
			workChannel <- struct{}{}
			var fieldPartArr []string
			if index+space < len(fieldNameArr) {
				fieldPartArr = fieldNameArr[index : index+space]
			} else {
				fieldPartArr = fieldNameArr[index:]
			}
			queryRes, err := i.QueryPointsByParam(measurement, tag, ConstructFieldQueryFilterStr(fieldPartArr))
			if err != nil {
				retErr := errors.New("query tsdb failed: %v", err)
				log.Error("error happened when query tsdb :%v", err)
				errChannel <- retErr
				return
			}
			resChannel <- queryRes
			<-workChannel
			wg.Done()
		}(index)
		index += space
	}
	wg.Wait()
	close(errChannel)
	close(resChannel)
	if len(errChannel) != 0 {
		var errMsg string
		for err := range errChannel {
			errMsg = fmt.Sprintf("%s, %s", errMsg, err)
		}
		return nil, errors.New("error when query QueryPointsByParamConcurrence: %v ", errMsg)
	}
	for res := range resChannel {
		resArr = append(resArr, res)
	}
	return resArr, nil
}

func (i *InfluxdbApi) QueryAllPointsByParam(measurement, tag string) (*api.QueryTableResult, error) {

	//如果没有指定时间，默认时间区间是：两个月前到当前时间
	end := time.Now()
	start := time.Date(end.Year(), end.Month()-2, end.Day(), end.Hour(), end.Minute(), end.Second(), end.Nanosecond(), time.Local)
	fieldStr := ""
	return i.QueryDataByParms(measurement, tag, fieldStr, start, end)
}

func (i *InfluxdbApi) DeletePointsByParam(measurement, tag string) error {
	startTime := time.Unix(0, 0)
	endTime := time.Now()
	deletePredicate := `_measurement="` + measurement + `"`
	if tag != "" {
		deletePredicate += ` and ` + tag
	}

	err := i.DeletePoints(startTime, endTime, deletePredicate)
	if err != nil {
		log.Error("error happened when delete points: %v", err)
		return err
	}

	return nil
}

//查询数据,add by fairzhang
func (i *InfluxdbApi) QueryDataByParmsMap(measurement string, tags map[string]string, fields []string, start_time, end_time time.Time) (*api.QueryTableResult, error) {

	tagStr := ConstructTagQueryFilterStr(tags) //如果tag为空将返回空字符串
	fieldStr := ConstructFieldQueryFilterStr(fields)
	return i.QueryDataByParms(measurement, tagStr, fieldStr, start_time, end_time)
}

func (i *InfluxdbApi) QueryDataByParms(measurement, tagStr, fieldStr string, start_time, end_time time.Time) (*api.QueryTableResult, error) {
	start := start_time.UTC().Format("2006-01-02T15:04:05Z") //转换为UTC格式
	end := end_time.UTC().Format("2006-01-02T15:04:05Z")
	var queryStr string
	queryStr = `from(bucket:"` + i.Bucket + `")
			|> range(start: ` + start + `, stop: ` + end + ` )`
	if measurement != "" {
		queryStr += `|> filter(fn: (r) => r._measurement == "` + measurement + `")`
	}
	if tagStr != "" { //tagStr为空，如果放入查询语句中，将得不到结果，如果为空，应按照else中的方式构造查询语句
		queryStr += `|> filter(fn: (r) => ` + tagStr + `)`
	}
	if fieldStr != "" {
		queryStr += `|> filter(fn: (r) => ` + fieldStr + `)`
	}
	log.Debug("query str: %v", queryStr)
	return i.QueryPoints(queryStr)
}

//构造过滤器字符串，add by fairzhang
func ConstructTagQueryFilterStr(query map[string]string) string {
	filterArr := make([]string, 0)
	for key, value := range query {
		kvStr := fmt.Sprintf("r.%s==\"%s\"", key, value)
		filterArr = append(filterArr, kvStr)
	}
	filterStr := strings.Join(filterArr, " and ")
	return filterStr
}

func ConstructFieldQueryFilterStr(query []string) string {
	filterArr := make([]string, 0)
	for _, value := range query {
		kvStr := fmt.Sprintf("r._field==\"%s\"", value)
		filterArr = append(filterArr, kvStr)
	}
	filterStr := strings.Join(filterArr, " and ")
	return filterStr
}

//将数据保存到InfluxDB
func (client *InfluxdbApi) WriteDataToTsDB(measurement string, field map[string]interface{}, tag map[string]string) error {
	writer := client.GetMeasurementPoint(measurement)
	if writer == nil {
		return errors.New("measurement is not valid")
	}
	//写入时，tag可以为空，但是field不能为空
	for key, value := range tag {
		if strings.Compare(value, "") == 0 {
			continue
		}
		writer.AddTag(key, value)
	}
	for key, value := range field {
		writer.AddField(key, value)
	}
	if len(field) == 0 {
		writer.AddField("field", 1.0)
	}
	now := time.Now()
	writer.SetTime(now)
	err := client.WritePoint(writer)
	if err != nil {
		return errors.New(fmt.Sprintf("write failed: %v", err))
	}
	//log.Debug("Write to influxdb:measurement:%v|tag:%v|field:%v success at time:%v", measurement, tag, field, now)
	return nil
}

func (client *InfluxdbApi) DeleteMeasurement(measurement string) error {
	return client.DeletePointsByParam(measurement, "")
}
