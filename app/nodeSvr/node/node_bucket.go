package node

import (
	"context"
	"fmt"
	"log"
	"os"
	"oss/app"
	"oss/lib/base"
	_ "oss/lib/base"
	_ "time"
)

//创建bucket
func (node *NodeSvr) CreateBucketFolder(ctx context.Context, req *app.BucketFolderReq, rsp *app.BucketFolderRsp) error {
	rsp.NodeId = req.NodeId
	rsp.BucketId = req.BucketId

	rsp.Ret = base.RET_ERROR
	rootPath := req.RootPath
	bucketPath := req.BucketId
	dir := rootPath + "/" + bucketPath
	//创建目录
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		rsp.NodeId = req.NodeId
		log.Fatalf("error creating filepath: %v", err)
		return fmt.Errorf("create bucketfloder: %v failed: %v", dir, err)
	}
	rsp.Ret = base.RET_OK
	return nil
}

//创建bucket
func (node *NodeSvr) DeleteBucketFolder(ctx context.Context, req *app.BucketFolderReq, rsp *app.BucketFolderRsp) error {
	rsp.Ret = base.RET_ERROR
	rootPath := req.RootPath
	bucketPath := req.BucketId
	dir := rootPath + "/" + bucketPath
	//删除目录
	err := os.RemoveAll(dir)
	if err != nil {
		rsp.NodeId = req.NodeId
		log.Fatalf("error creating filepath: %v", err)
		return fmt.Errorf("remove bucketfloder: %v failed: %v", dir, err)
	}

	rsp.Ret = base.RET_OK
	return nil
}
