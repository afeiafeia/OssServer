package dataHandle

//block碎片整理(暂未完成)
type BlockCompressReq struct {
	NodeId    uint64         `json:"nodeId"`    //所属节点
	FilePath  string         `json:"filePath"`  //block的文件路径
	DelRegion []DeleteRegion `json:"delRegion"` //删除区间
}

type DeleteRegion struct {
	StOffset uint64 `json:"StOffset"` //删除区间的起始处偏移量
	EdOffset uint64 `json:"EdOffset"` //删除区间的终止处偏移量
}

//桶的创建、查询、删除的回复
type BlockCompressRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}
