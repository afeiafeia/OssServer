package app

//桶的创建、查询、删除的请求
type BucketReq struct {
	BucketName string `json:"bucketName"` //桶名称
	OwnerId    string `json:"ownerId"`    //所属租户的id(accessId)
	TimeStamp  uint64 `json:"timeStamp"`  //时间戳:上传时的时间戳(秒级别)
}

//向用户返回bucket下的对象数量
type ObjectCountRsp struct {
	Ret         int    `json:"ret"`         //回复状态码
	ErrorMsg    string `json:"errorMsg"`    //具体错误信息
	ObjectCount int    `json:"objectCount"` //对象数量
}

//桶的创建、删除的回复
type BucketRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}

//桶的查询的回复
type BucketQueryRsp struct {
	Ret         int    `json:"ret"`         //回复状态码
	ErrorMsg    string `json:"errorMsg"`    //具体错误信息
	ObjectCount uint64 `json:"objectCount"` //bucket下的对象数量
}

//暂不使用
type BucketFolderReq struct {
	NodeId   uint64 `json:"nodeId"`   //所落节点
	RootPath string `json:"rootPath"` //节点的根目录
	BucketId string `json:"bucketId"` //文件夹名称，将在节点的根目录下创建一个该名称的文件夹
}

//暂不使用
type BucketFolderRsp struct {
	Ret      uint32 `json:"ret"`      //回复状态码
	NodeId   uint64 `json:"nodeId"`   //节点id
	BucketId string `json:"bucketId"` //文件夹名称，
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}
