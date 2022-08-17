package app

//连接时的验证请求
type AccessVerify struct {
	AccessId  string `json:"accessId"`  //用户id
	AccessKey string `json:"accessKey"` //用户密钥
}

//连接验证的回复，后期可在此加入EC码的分片策略，以供sdk端调整EC码分片数量
type AccessVerifyRsp struct {
	Ret      int    `json:"ret"`      //回复状态码,为0表示验证通过，否则不通过
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}
