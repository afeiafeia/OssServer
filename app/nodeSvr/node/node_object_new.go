package node

import (
	"context"
	"fmt"
	"os"
	"oss/app"
	"oss/codec"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"path/filepath"
	_ "time"
)

//保存数据
func (node *NodeSvr) SaveObjectSliceData(ctx context.Context, objectData *[]byte, rsp *app.ObjSliceSaveRsp) error {

	log.Info("New SaveObjectSliceData without json")
	//头部的taskId中记录了objectId
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	taskId := header.GetTaskId()
	//可以由此id解析出blockPath和offset
	blockPath, objId, version, off, sliceNum, nodeId := tcaplus_api.DecodeLocationId(taskId)

	rsp.Ret = base.RET_ERROR
	rsp.ObjectId = objId
	rsp.Num = (uint32)(sliceNum)
	rsp.NodeId = nodeId
	rsp.Version = (int32)(version)
	filePath := blockPath
	//先创建目录
	dir := filepath.Dir(filePath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		fmt.Printf("creating folder: %v failed: %v", dir, err)
		return nil
	}
	//打开或者创建文件
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Error("error opening file: %v", err)
		return nil
	}

	offset := (int64)(off)
	defer f.Close()
	//在指定位置写入
	size, err := f.WriteAt(*objectData, offset)
	if err != nil {
		return nil
	}
	if size != len(*objectData) {
		return nil
	}
	rsp.Ret = base.RET_OK

	log.Debug("Save Success")
	return nil
}
