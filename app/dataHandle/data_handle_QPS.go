package dataHandle

import (
	_ "oss/lib/base"
	"time"
)

const (
	//全链路QPS采样频率，每3秒1次
	QPSSimplePeriodic = 3
)

var QPSTimer *time.Timer

//在经过sendAddNodeInfo向dataHandle发送AddNodeInfo请求后，将收到回复，此函数处理回复
func (dataHandler *DataHandle) uploadSimpleQPSInfo() {

	info := dataHandler.base.GetReqRspInfo()
	Server := "DataHandle"

	dataHandler.GetInfluxdbClient().CalServerQPS(info, Server)
	expire := QPSSimplePeriodic * time.Second
	QPSTimer.Reset(expire)

	//log.Error("upload dataHandle's QPS info:%v", info)
}
