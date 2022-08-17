package dataHandle

import (
	"context"
	"fmt"
	"oss/app"
	"oss/codec"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"reflect"
	"time"

	"github.com/rangechow/errors"
)

const (
	ProjectName = "TK"
)

//client经由dispatch转发过来的创建bucekt的请求
func (dataHandler *DataHandle) BucketCreate(ctx context.Context, req *app.BucketReq, rsp *app.BucketRsp) error {
	rsp.Ret = base.RET_ERROR
	//先查询该用户名下的bucket,判断是否有同名的bucket，有则直接返回
	bucketReq := tcaplus_uecqms.NewTb_Bucket()
	bucketReq.ProjectName = ProjectName
	bucketReq.OwnerId = req.OwnerId
	bucketReq.Name = req.BucketName
	bucket, err := dataHandler.GetTcaplusClient().GetBucket(bucketReq)
	if err != nil && !errors.Is(err, tcaplus_api.NO_RECORD) {
		log.Error("Get bucket failed:%v", err)
		return fmt.Errorf("Get bucket failed:%v", err)
	}
	if bucket != nil {
		//该用户已经存在同名bucket
		rsp.ErrorMsg = "user has bucket with same name!"
		return nil
	}
	//如果该用户没有同名bucket，进行bucket的创建
	timeStamp := time.Now().Unix()
	bucketRecord := tcaplus_api.ConstructBucketRecord(req.OwnerId, req.BucketName, (uint64)(timeStamp))
	err = dataHandler.GetTcaplusClient().InsertBucket(bucketRecord)
	if err != nil {
		log.Warn("insert bucket failed: %v", err)
		return nil
	}
	log.Debug("Create bucket:%v success!", bucketRecord)

	//创建bucket后，将其添加到dataHandle的allUserIdBucketIdMap中
	dataHandler.mu.Lock()
	dataHandler.allUserIdBucketIdMap[req.OwnerId][bucketRecord.BucketId] = (uint64)(1)
	dataHandler.mu.Unlock()
	rsp.Ret = base.RET_OK
	return nil
}

//client经由dispatch转发过来的bucket删除的请求
//删除一个bucket,req中的BucketName、OwnerId要有有效值
func (dataHandler *DataHandle) BucketDelete(ctx context.Context, req *app.BucketReq, rsp *app.BucketRsp) error {
	var err error
	rsp.Ret = base.RET_ERROR
	//找到所有节点下与req匹配的bucket,删除
	//根据ownerId和name查找该桶的信息，查看在哪些节点上有该bucket的信息，在相应节点删除，如果有节点处于下线状态，需要记录待删除状态，等其再次上线，将数据删除

	//查找该用户的桶bucketName放在了哪些节点
	record := tcaplus_uecqms.NewTb_Bucket()
	record.ProjectName = ProjectName
	record.OwnerId = req.OwnerId
	record.Name = req.BucketName
	bucketId := tcaplus_api.EncodeBucketId(req.OwnerId, req.BucketName)
	record, err = dataHandler.GetTcaplusClient().GetBucket(record)
	if err != nil || record == nil {
		log.Error("Didn't find bucket:|OwnerId:%v|BucketName:%v|Reason:%v", req.OwnerId, req.BucketName, err)
		return fmt.Errorf("Didn't find bucket:%v", err)
	}
	dataHandler.mu.Lock()
	allNode := dataHandler.aliveNodeMap
	dataHandler.mu.Unlock()
	for nodeId, nodeInfo := range allNode {
		//将bucketId记录到bucket_need_delete表中
		bucketNeedDelete := tcaplus_uecqms.NewTb_Bucket_Need_Delete()
		bucketNeedDelete.Flag = "Delete"
		bucketNeedDelete.NodeId = nodeId
		bucketNeedDelete.BucketId = bucketId
		err = dataHandler.GetTcaplusClient().InsertBucketNeedDelete(bucketNeedDelete)
		if err != nil {
			log.Error("Insert bucket need delete:%v failed:%v", bucketNeedDelete, err)
			return fmt.Errorf("Delete bucket failed:%v", err)
		}
		//向节点发送删除文件夹的请求
		//经Dispatch向node发送消息，删除bucketId文件夹
		if nodeInfo.State != 1 {
			//如果节点当前不是存活的，跳过
			continue
		}
		//如果节点是存活的，向节点发送删除文件夹的请求
		bucketFolderInfo := app.BucketFolderReq{
			NodeId:   nodeId,
			RootPath: nodeInfo.Root,
			BucketId: bucketId,
		}
		go dataHandler.sendBucketDeleteReq(nodeId, &bucketFolderInfo)
	}

	rsp.Ret = 0
	return nil
}

//发往Dispatch,Dispatch将根据其内的NodeId,发往指定节点
func (dataHandler *DataHandle) sendBucketDeleteReq(nodeId uint64, bucketInfo *app.BucketFolderReq) {

	payloadBytes, err := dataHandler.base.GetCodeC().Marshal(reflect.ValueOf(bucketInfo))
	if err != nil {
		log.Error("Encoding bucketInfo: %v failed: %v", *bucketInfo, err)
		return
	}
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("BucketFolderDelete")
	//编码头部
	headerBytes, errB := dataHandler.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
	}
	var session *base.Session
	for _, s := range dataHandler.base.GetSessions() {
		session = s
		break
	}
	errW := session.Write(headerBytes, payloadBytes)
	if errW != nil {
		//fmt.Printf("failed to write req msg with MsgId:%v ,err is:%v\n", header.GetMsgId(), errW)
		log.Error("failed to write req msg,err is:%v\n", errW)
		log.Error("fail to delete bucket %v", *bucketInfo)
		return
	}
}

//dataSaveSvr删除文件夹后，会向dataHandle发送一个删除结果的报文
//dataHandle将根据此结果觉得是否删除bucket_need_delete的记录
func (dataHandler *DataHandle) DeleteBucketFolder(ctx context.Context, rsp *app.BucketFolderRsp) error {

	if rsp.Ret != base.RET_OK {
		//如果dataSaveSvr删除失败，输出日志，并告警，查看问题
		log.Error("BucketFolder:|NodeId:%v|BucketId:%v delete failed:%v", rsp.NodeId, rsp.BucketId)
		return nil
	}

	//将bucketneeddelete表中的记录删除
	bucketNeedDelete := tcaplus_uecqms.NewTb_Bucket_Need_Delete()
	bucketNeedDelete.Flag = "Delete"
	bucketNeedDelete.NodeId = rsp.NodeId
	bucketNeedDelete.BucketId = rsp.BucketId
	err := dataHandler.GetTcaplusClient().DeleteBucketNeedDelete(bucketNeedDelete)
	if err != nil {
		log.Fatal("BucketFolder:|NodeId:%v|BucketId:%v deleted but record delete failed:%v", rsp.NodeId, rsp.BucketId, err)
		return nil
	}
	return nil
}

func (dataHandler *DataHandle) BucketQuery(ctx context.Context, req *app.BucketReq, rsp *app.BucketQueryRsp) error {
	rsp.Ret = base.RET_ERROR
	//先查询该用户名下的bucket,判断是否有同名的bucket，有则直接返回
	bucketTableName := "tb_bucket"
	objCount, err := dataHandler.GetTcaplusClient().GetTableRecordCount(bucketTableName)
	if err != nil {
		log.Error("Get Count failed:%v", err)
		return fmt.Errorf("Get Count failed:%v", err)
	}

	rsp.ObjectCount = (uint64)(objCount)
	rsp.Ret = base.RET_OK
	return nil
}

//周期性检查bucket_need_delete表，删除bucket
func (dataHandler *DataHandle) deleteBucketPeriodically() {

	defer func() {
		expire := calExpireTime()
		DeleteTimer.Reset(expire)
	}()
	dataHandler.mu.Lock()
	allNode := dataHandler.aliveNodeMap
	dataHandler.mu.Unlock()

	bucketIdSet, err := dataHandler.GetTcaplusClient().GetBucketNeedDelete()
	if err != nil {
		log.Error("Get Bucket need delete failed:%v", err)
	}
	for _, bucket := range bucketIdSet {

		//向节点发送删除文件夹的请求
		//经Dispatch向node发送消息，删除bucketId文件夹
		nodeInfo := allNode[bucket.NodeId]
		if nodeInfo.State == 1 {
			//节点存活，向节点发送删除文件夹的请求
			bucketFolderInfo := app.BucketFolderReq{
				NodeId:   bucket.NodeId,
				RootPath: nodeInfo.Root,
				BucketId: bucket.BucketId,
			}
			go dataHandler.sendBucketDeleteReq(bucket.NodeId, &bucketFolderInfo)
		}

		//获取bucketId的object
		//查找该bucketId下的所有元数据记录
		//根据bucketId查询对象
		objMeta := tcaplus_uecqms.NewTb_Object_Metadata()
		objMeta.BucketId = bucket.BucketId
		index := "Index_B"
		//每次最多获取100条
		objSet, errM := dataHandler.GetTcaplusClientObject().GetMetadataByIndex(objMeta, index)
		if errM != nil {
			fmt.Printf("Get metadata of bucket:%v failed:%v\n", objMeta.BucketId, errM)
		}
		objDeleteFlag := true
		for _, obj := range objSet {
			////遍历，获取每一个对象的分片记录，删除
			//objId := obj.ObjectId
			//slice := tcaplus_uecqms.NewTb_Object_Slice()
			//
			//slice.ObjectId = obj.ObjectId
			//index := "Index_Obj"
			//err := dataHandler.GetTcaplusClient().DeleteObjectSliceByIndex(slice, index)
			//if err != nil {
			//	objDeleteFlag = false
			//	fmt.Printf("Get slice of objectId:%v\n", objId)
			//	continue
			//}
			//在分片删除成功后，将metadata删除
			err = dataHandler.GetTcaplusClientObject().DeleteObjectMetadata(&obj)
			if err != nil {
				objDeleteFlag = false
				log.Error("Delete objectMetadata:|BucketId:%v|ObjectId:%v failed:%v", obj.BucketId, obj.ObjectId, err)
				continue
			}
		}
		if objDeleteFlag {
			//在metadata和slice都删除成功后，将bucket删除
			record := tcaplus_uecqms.NewTb_Bucket()
			record.ProjectName = ProjectName
			record.OwnerId, record.Name = tcaplus_api.DecodeBucketId(bucket.BucketId)
			record, err = dataHandler.GetTcaplusClient().GetBucket(record)
			if err != nil {
				log.Error("Get Bucket:|BucketId:%v failed:%v", bucket.BucketId, err)
				continue
			}
			err = dataHandler.GetTcaplusClient().DeleteBucket(record)
			if err != nil {
				log.Error("Delete Bucket:|BucketId:%v failed:%v", bucket.BucketId, err)
				continue
			}
		}
	}
}

//获取指定bucket下的object数量
func (dataHandler *DataHandle) GetObjectCount(ctx context.Context, req *app.BucketReq, rsp *app.ObjectCountRsp) error {

	rsp.Ret = base.RET_ERROR
	userId := req.OwnerId
	bucketName := req.BucketName

	bucketRecord := tcaplus_uecqms.NewTb_Bucket()
	bucketRecord.ProjectName = ProjectName
	bucketRecord.BucketId = tcaplus_api.EncodeBucketId(userId, bucketName)
	index := "Index_Pno"
	count, err := dataHandler.GetTcaplusClient().GetObjectCountOfBucket(bucketRecord, index)
	if err != nil {
		log.Error("Get object count of UserId:%v|BucketName:%v|failed:%v", userId, bucketName, err)
		return fmt.Errorf("Get count failed")
	}

	rsp.ObjectCount = count
	rsp.Ret = base.RET_OK
	return nil

}
