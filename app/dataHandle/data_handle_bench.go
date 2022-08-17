//此处实现DBHandler的函数，用于业务逻辑处理：一般是对DBHandler所管理的数据库进行读写操作

package dataHandle

import (
	"context"
)

// 上报数据请求
type ECHOReq struct {
	Req int `json:"req"`
}

// 上报数据回包
type ECHORsp struct {
	Ret int `json:"ret"`
}

//用于测试
// 数据上报处理逻辑
func (dataHandler *DataHandle) ECHOFun(ctx context.Context, req *ECHOReq, rsp *ECHORsp) error {
	rsp.Ret = 200
	return nil
}
