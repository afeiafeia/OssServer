package node

import (
	"oss/dao/influxdb"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"time"
	_ "time"

	"github.com/spf13/viper"
)

type NodeSvr struct {
	//其所持有的唯一连接是Dispatch
	//底层服务框架
	base *base.Base

	//时序数据库influxdb，用于上报该节点的QPS、流量等信息
	influxdb_client *influxdb.InfluxdbApi
	//节点在系统中的id
	id              uint64
	connectFlagChan chan int8
}

func (nodeSvr *NodeSvr) Init(b *base.Base) error {
	nodeSvr.base = b

	nodeSvr.connectToInfluxDB()
	log.Info("Connect to influxdb success")

	//定时采样上报：统计QPS
	expire := QPSSimplePeriodic * time.Second
	QPSTimer = time.AfterFunc(expire, nodeSvr.uploadSimpleQPSInfo)
	return nil
}

func (nodeSvr *NodeSvr) GetInfluxdbClient() *influxdb.InfluxdbApi {
	return nodeSvr.influxdb_client
}

//influxdb连接时，不存在连接不上的情况
//即使连接参数不对，db_handler.influxdb_client及其client也均不为空，但后期会在调用接口时返回连接失败的错误
//因此influxdb无法通过连接的结果判断是否连接成功，所有influxdb不使用重连机制
func (nodeSvr *NodeSvr) connectToInfluxDB() {
	nodeSvr.influxdb_client = influxdb.NewInfludbClient(
		viper.GetString("DAO.INFLUXDB.IN_OGR"),
		viper.GetString("DAO.INFLUXDB.IN_TOKEN"),
		viper.GetString("DAO.INFLUXDB.IN_ADDR"),
		viper.GetString("DAO.INFLUXDB.IN_BUCKET"))

	return
}
