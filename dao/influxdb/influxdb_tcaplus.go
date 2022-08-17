//用于上报对tcaplus的访问的QPS

package influxdb

import (
	"fmt"
)

const (
	tcaplusMeasurement = "Tcaplus"
	Times              = "Times"
	AccessCount        = "AccessCount" //访问次数
	Client             = "Client"      //客户端：为减少单个客户端的访问次数，目前对bucket和object的操作，使用两个客户端分别进行
)

type TcaplusAccess struct {
	Client      string //所属客户端
	SimpleTime  int64  //采样时间
	AccessCount int64  //请求的总数
	QPS         int64  //QPS
}

//将FrameInfoData写入数据库
func (client *InfluxdbApi) UploadTcaplusAccessInfo(accessInfo TcaplusAccess) error {

	measurement := tcaplusMeasurement

	tags := make(map[string]string, 0)
	tags[Client] = accessInfo.Client

	fields := make(map[string]interface{}, 0)
	fields[Times] = accessInfo.SimpleTime
	fields[AccessCount] = accessInfo.AccessCount
	fields[QPS] = accessInfo.QPS
	err := client.WriteDataToTsDB(measurement, fields, tags)
	if err != nil {
		return fmt.Errorf("failed to save request data:%v\n", err)
	}
	return nil
}
