package node

import (
	_ "oss/lib/base"
	"time"
)

const (
	//QPS数据采样频率，每3秒1次
	QPSSimplePeriodic = 3
)

var QPSTimer *time.Timer

//在经过sendAddNodeInfo向dataHandle发送AddNodeInfo请求后，将收到回复，此函数处理回复
func (node *NodeSvr) uploadSimpleQPSInfo() {

	info := node.base.GetReqRspInfo()
	Server := "NodeSvr"

	node.GetInfluxdbClient().CalServerQPS(info, Server)
	expire := QPSSimplePeriodic * time.Second
	QPSTimer.Reset(expire)
}
