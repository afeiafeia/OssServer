package node

import (
	"context"
	"fmt"
	"os"
	"oss/app"
	"oss/codec"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"path/filepath"
	"reflect"
	_ "time"
)

//数据分片的类型
type ObjType uint8

const (
	ObjectData ObjType = iota //对象实际数据
	ObjECCode                 //对象的EC码
)

type ObjSlice struct {
	SliceType ObjType `json:"sliceType"` //对象类型，可以是实际存储对象或者EC码
	Num       uint32  `json:"num"`       //编号(用于按序恢复原对象)
	Data      []byte  `json:"data"`      //分片数据
}

//保存数据
func (node *NodeSvr) SaveObjectSliceData_Old(ctx context.Context, req *app.ObjSliceSaveReq, rsp *app.ObjSliceSaveRsp) error {

	log.Info("start save slice info:ObjectId:%v|NodeId:%v|Num:%v|BlockPath:%v|Offset:%v", req.ObjectId, req.NodeId, req.Num, req.BlockPath, req.Offset)
	rsp.Ret = base.RET_ERROR
	rsp.ObjectId = req.ObjectId
	rsp.Num = req.Num
	rsp.NodeId = req.NodeId
	rsp.Version = req.Version
	filePath := req.BlockPath
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

	offset := (int64)(req.Offset)
	defer f.Close()
	//在指定位置写入
	size, err := f.WriteAt(req.Data, offset)
	if err != nil {
		return nil
	}
	if size != len(req.Data) {
		return nil
	}
	rsp.Ret = base.RET_OK

	log.Info("save ObjectId:%v|SliceNum:%v|BlockPath:%v|Offset:%v success", req.ObjectId, req.Num, req.BlockPath, req.Offset)
	log.Debug("Save Success")
	return nil
}

func (node *NodeSvr) ObjectDataQuery(ctx context.Context, req *app.ObjectSliceLocation, rsp *app.ObjSliceGetRsp) error {

	filePath := req.BlockPath
	log.Debug("read file:%v", filePath)
	rsp.Ret = base.RET_ERROR
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Error("error opening file: %v", err)
		return nil
	}

	offset := (int64)(req.OffSet)
	length := (int)(req.Length)
	log.Debug("Read from:%v size is:%v", offset, length)
	defer f.Close()
	//在指定位置读取指定长度字节
	data := make([]byte, length)
	readSize, err := f.ReadAt(data, offset)
	if err != nil {
		return nil
	}
	if readSize != length {
		return nil
	}

	////进行MD5校验
	//md5Inst := md5.New()
	//md5Inst.Write(data)
	//md5Str := hex.EncodeToString(md5Inst.Sum(nil))
	//log.Debug("md5Str:%v", md5Str)
	//log.Debug("req's md5:%v", req.Md5)
	//if strings.Compare(md5Str, req.Md5) != 0 {
	//	log.Error("Md5 is not equal!")
	//	return nil
	//}
	//如果成功，将分片的数据转发由dispatch转发给client，头部taskId记为rap.ObjectId
	//获取当前session
	s := ctx.Value("session")
	session := s.(*base.Session)
	dataRsp := &app.ObjectQueryRsp{
		Ret:      base.RET_OK,
		DataType: app.ObjectSliceData,
		Slice: app.ObjSlice{
			Type: req.Type,
			Num:  req.Num,
			Data: data,
		},
	}
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	simpleHeader := *header
	log.Debug("read file's data length is:%v", len(data))
	objId := req.ObjectId
	err = node.sendDataToClient(simpleHeader, objId, dataRsp, session)
	if err != nil {
		return nil
	}
	log.Debug("Send slice data to client success:|Ret:%v|DataType:%v|SliceType:%v|SliceNum:%v", dataRsp.Ret, dataRsp.DataType, dataRsp.Slice.Type, dataRsp.Slice.Num)
	rsp.ObjectId = req.ObjectId
	rsp.Num = req.Num
	rsp.NodeId = req.NodeId
	rsp.Type = req.Type
	rsp.Ret = base.RET_OK
	return nil
}

func (node *NodeSvr) sendDataToClient(header codec.SimpleHeader, objId string, req *app.ObjectQueryRsp, session *base.Session) error {

	payloadBytes, err := node.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *req, err)
		return err
	}
	payloadLen := len(payloadBytes)
	//向dataHandle发送获取分片数据的请求
	header.SetPayloadLength((int32)(payloadLen))
	header.SetTaskId(objId)
	header.SetCmd("ObjectSliceData")
	headerBytes, errB := node.base.GetCodeC().GetHeaderBytes(&header)
	if errB != nil {
		log.Error("GetHeaderBytes failed:%v\n", errB)
		return fmt.Errorf("GetHeaderBytes failed%v\n", errB)
	}
	//因为是从数据库中查询，所以可以发送给其所连接的任一个dataHandle
	err = session.Write(headerBytes, payloadBytes)
	if err != nil {
		log.Error("GetHeaderBytes failed:%v\n", errB)
		return err
	}

	return nil
}
