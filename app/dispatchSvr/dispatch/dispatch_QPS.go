package dispatch

import (
	_ "oss/lib/base"
	"time"
)

const (
	//采样频率，每3秒1次
	QPSSimplePeriodic = 3
)

var QPSTimer *time.Timer

//在经过sendAddNodeInfo向dataHandle发送AddNodeInfo请求后，将收到回复，此函数处理回复
func (dispatch *Dispatch) uploadSimpleQPSInfo() {

	info := dispatch.base.GetReqRspInfo()
	Server := "Dispatch"

	dispatch.GetInfluxdbClient().CalServerQPS(info, Server)
	expire := QPSSimplePeriodic * time.Second
	QPSTimer.Reset(expire)

}
