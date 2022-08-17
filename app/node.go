package app

//节点更新的请求，当dispatchSvr启动或者服务运行期间加入新dataSaveSvr节点，dispatchSvr会将信息发送给dataHandle，dataHandle更新本地对存活dataSaveSvr节点的记录
//如果是新引入的dataSaveSvr节点，还会更新到tcaplus中
type NodeInfoUpdateReq struct {
	Node []NodeInfo `json:"node"`
}

//一个dataSaveSvr节点的具体信息
type NodeInfo struct {
	Id        uint64 `json:"id"`    //节点id
	Ip        string `json:"ip"`    //ip地址
	Port      uint32 `json:"port"`  //端口
	Root      string `json:"root"`  //根目录//当删除时，值可为空
	Space     uint64 `json:"space"` //可用空间大小//当删除时，值可为0
	BlockSize uint64 `json:"blockSize"`
}

//当节点在dataHandle更新完成以及更新到tcaplus中(如果是新引入的节点)，dataHandleSvr会向dispatchSvr发送处理的结果
type NodeBriefInfoRsp struct {
	Ret          int         `json:"ret"`          //回复状态码，是否更新成功
	ErrorMsg     string      `json:"errorMsg"`     //具体错误信息
	NodeBriefSet []NodeBrief `json:"nodeBriefSet"` //节点id
}

type NodeBrief struct {
	Ret  int    `json:"ret"`  //标识节点的记录是否正确记录到tcaplus中
	Id   uint64 `json:"id"`   //节点id
	Ip   string `json:"ip"`   //ip地址
	Port uint32 `json:"port"` //端口
}

//当dataSaveSvr启动时，会读取配置文件中的DispatchSvrInfo，如果有有效信息，会主动连接dispatchSvr并告知自己的存储信息
type DispatchSvrInfo struct {
	Ip        string `json:"ip"`
	Root      string `json:"root"`
	Space     string `json:"space"`
	SpaceUnit string `json:"spaceUnit"`
	BlockSize string `json:"blockSize"`
	BlockUnit string `json:"blockUnit"`
}

//当服务运行期间有dataSaveSvr断开连接时，dispatchSvr会将信息发送给dataHandle,dataHandle更新缓存的dataSaveSvr节点信息
type NodeDisable struct {
	Id uint64 `json:"id"` //节点id
}
