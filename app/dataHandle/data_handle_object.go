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
	"strconv"
	"strings"

	"github.com/rangechow/errors"
)

//client发送的经由dispatch转发过来的对象元数据
func (dataHandler *DataHandle) UploadMetadata(ctx context.Context, req *app.ObjectMetadataUploadReq, rsp *app.ObjectMetadataUploadRsp) error {

	////////上报至influxdb(暂时取消上报)
	////log.Debug("start upload to influxdb")
	//go dataHandler.uploadMetadataRequest(ctx, req)
	////log.Debug("call  uploadMetadataRequest finish")
	//defer func() {
	//	dataHandler.uploadMetadataResult(ctx, rsp)
	//}()
	rsp.Ret = base.RET_ERROR
	objectId := tcaplus_api.EncodeObjectId(req.UserId, req.BucketName, req.ObjName)
	//log.Warn("Receive metadata of object:%v ", objectId)
	rsp.TaskId = objectId
	//log.Info("Receive metadata:%v from dispatchSvr!", *req)
	//根据用户id和桶名称查找桶id
	bucketId := tcaplus_api.EncodeBucketId(req.UserId, req.BucketName)

	//log.Warn("Start get bucket by index")
	isExistBucket := false

	dataHandler.mu.Lock()
	if _, ok := dataHandler.allUserIdBucketIdMap[req.UserId][bucketId]; ok {
		isExistBucket = true
	}
	dataHandler.mu.Unlock()
	if !isExistBucket {
		rsp.ErrorMsg = "there is no this bucket!"
		log.Error("no bucket")
		return nil
	}
	log.Info("User:%v has bucket:%v!", req.UserId, req.BucketName)
	//构造objectId
	//收到object的元数据
	log.Info("receicv object:%v's metadata!", objectId)

	dataHandler.mu.Lock()
	if _, ok := dataHandler.objectDataInfoMap[objectId]; ok {
		//如果分片信息已经存在，不能再上传
		dataHandler.mu.Unlock()
		rsp.ErrorMsg = "Object is handling"
		log.Warn("Object:%v is handling", req)
		return nil
	}
	dataHandler.objectDataInfoMap[objectId] = new(ObjectDataInfo)
	curDataInfo := dataHandler.objectDataInfoMap[objectId]
	dataHandler.mu.Unlock()

	curDataInfo.metaData = req
	//构造分片落地方案
	sliceInfo, err := dataHandler.constructStoragePlan(req, bucketId, objectId)
	if err != nil {
		log.Error("construct storage plan for object:%v failed:%v", objectId, err)
		return nil
	}
	log.Debug("Construct storage plan success:%v", sliceInfo)

	curDataInfo.sliceInfo = sliceInfo
	curDataInfo.retryTimes = 1
	curDataInfo.stopChan = make(chan int8, 1)

	rsp.Ret = base.RET_OK

	//此处持有object到dispatch的连接
	s := ctx.Value("session")
	session := s.(*base.Session)
	//log.Debug("Create map from objectId:%v to session:%v", objectId, session)
	dataHandler.mu.Lock()
	dataHandler.objectDispatchMap[objectId] = session
	dataHandler.mu.Unlock()

	log.Info("Storage metadata success!")

	//如果没有收到对该对象最终落盘结果的确认，将在15秒后删除该记录
	//time.AfterFunc(15*time.Second, dataHandler.eraseDataInfo)
	//log.Warn("metadata of object:%v handle success", objectId)
	return nil

}

//dispatch收到分片数据后，将请求分片的落地方案，此处取出落地方案回复给dispatch
func (dataHandle *DataHandle) SliceStoragePlan(ctx context.Context, req *app.ObjectStoragePlanReq, rsp *app.ObjectStoragePlanRsp) error {

	rsp.Ret = base.RET_ERROR
	//所要获取对象的id
	objId := req.ObjectId
	log.Info("Response object:%v's storage plan to dispatchSvr...", objId)

	dataHandle.mu.Lock()
	curDataInfo, ok := dataHandle.objectDataInfoMap[objId]
	if !ok {
		//分片信息不在存在
		//回复失败
		dataHandle.mu.Unlock()
		log.Warn("Object:%v's slice storage plan is erased!", objId)
		return nil
	}
	dataHandle.mu.Unlock()

	userId, bucketName, objectName := tcaplus_api.DecodeObjectId(objId)
	rsp.UserId = userId
	rsp.BucketName = bucketName
	rsp.ObjName = objectName
	rsp.Times = curDataInfo.retryTimes
	rsp.SliceLocation = make([]app.ObjectSliceLocation, 0)

	for _, sliceLocation := range curDataInfo.sliceInfo {
		rsp.SliceLocation = append(rsp.SliceLocation, *sliceLocation)
	}

	rsp.Ret = base.RET_OK
	log.Info("Response object:%v's storage plan to dispatchSvr sucess", objId)
	log.Debug("Response object:%v's slice storage plan", objId)
	return nil

}

func constructMetadataRecord(meta *app.ObjectMetadataUploadReq, bucketId string) *tcaplus_uecqms.Tb_Object_Metadata {
	record := tcaplus_uecqms.NewTb_Object_Metadata()
	userId, bucketName := tcaplus_api.DecodeBucketId(bucketId)
	record.BucketId = bucketId
	record.ObjectName = meta.ObjName
	record.ObjectId = tcaplus_api.EncodeObjectId(userId, bucketName, meta.ObjName)
	record.ContentLength = meta.MetaInfo.ContentLength
	record.ContentType = meta.MetaInfo.ContentType
	record.ContentEncode = meta.MetaInfo.ContentEncode
	record.Suffix = meta.MetaInfo.Suffix
	record.UploadTime = meta.MetaInfo.UploadTime
	record.Md5 = meta.MetaInfo.Md5
	record.IsEncript = meta.MetaInfo.IsEncript
	record.EncriptAlgo = meta.MetaInfo.EncriptAlgo
	record.SliceDataCount = meta.MetaInfo.OriDataSliceCount
	//record.SliceDataCount = (uint32)(len(meta.MetaInfo.SliceInfo))
	//record.SliceECCodeCount = (uint32)(0)
	record.SliceECCodeCount = meta.MetaInfo.ECCodeSliceCount
	record.SliceCount = (int32)(len(meta.MetaInfo.SliceInfo))
	//record.SliceCount = (int32)(0)
	record.Expire = meta.MetaInfo.Expire
	return record
}

//构造落地方案
func (dataHandler *DataHandle) constructStoragePlan(meta *app.ObjectMetadataUploadReq, bucketId, objId string) (map[uint32]*app.ObjectSliceLocation, error) {

	var err error
	dataHandler.mu.Lock()
	//获取目前所有的存活节点
	nodeSet := dataHandler.aliveNodeMap
	dataHandler.mu.Unlock()
	//目前存活的节点
	nodeInfoSet := make([]*tcaplus_uecqms.Tb_Node_Info, 0)

	bucketInfoSet := make(map[uint64]*tcaplus_uecqms.Tb_Bucket_In_Node)
	validNodeCount := 0
	//每一个节点的block大小,key是节点id
	nodeBlockSize := make(map[uint64]uint64)

	for _, node := range nodeSet {
		if node.State == 0 {
			log.Warn("Node:%v is down", node)
			continue
		}
		//首先判断该节点下面是不是有bucketId文件夹，没有的话，在tcaplus中插入一条bucketIdInNode的记录
		bucketInfoInNode := tcaplus_uecqms.NewTb_Bucket_In_Node()
		bucketInfoInNode.NodeId = node.Id
		bucketInfoInNode.BucketId = bucketId
		bucketInfoInNodeRecord, errG := dataHandler.GetTcaplusClient().GetBucketInfoInNode(bucketInfoInNode)
		if errG != nil && !errors.Is(errG, tcaplus_api.NO_RECORD) {
			log.Warn("Get buctekInfoInNode:%v failed:%v", bucketInfoInNode, errG)
			continue
		}
		if bucketInfoInNodeRecord == nil {
			//如果当前节点下面没有bucketId文件夹，构造记录
			bucketInfoInNode.BlockPath = node.Root + "/" + bucketId + "/" + "block.1"
			bucketInfoInNode.BlockValidSpace = node.BlockSize

			err = dataHandler.GetTcaplusClient().InsertBucketInfoInNode(bucketInfoInNode)
			if err != nil {
				log.Warn("Insert bucket in node:%v failed", bucketInfoInNode, err)
				continue
			}
		} else {
			bucketInfoInNode = bucketInfoInNodeRecord
		}

		validNodeCount++
		bucketInfoSet[node.Id] = bucketInfoInNode
		nodeBlockSize[node.Id] = node.BlockSize
		nodeInfoSet = append(nodeInfoSet, node)
	}
	if validNodeCount == 0 {
		log.Error("No valid node!")
		return nil, fmt.Errorf("There is no valid node")
	}

	sliceLocationSet := make(map[uint32]*app.ObjectSliceLocation)
	//构造分片方案
	for i, sliceInfo := range meta.MetaInfo.SliceInfo {
		location := app.ObjectSliceLocation{}
		//先决定分片放入哪个节点
		index := (i % validNodeCount)
		nodeId := nodeInfoSet[index].Id
		location.NodeId = uint64(nodeId)
		//发送给dispatch的落地方案，也是tcaplus中的记录
		location.ObjectId = objId
		location.Type = sliceInfo.Type
		location.Num = sliceInfo.Num
		location.NodeId = uint64(nodeId)
		location.Md5 = sliceInfo.Md5
		//选择blockPath
		//首先获取block的剩余空间，看是否可以容纳数据
		blockRemainSize := bucketInfoSet[nodeId].BlockValidSpace
		if sliceInfo.Length > blockRemainSize {
			path := bucketInfoSet[nodeId].BlockPath
			str := strings.Split(path, ".")
			numStr := str[1]
			num, _ := strconv.Atoi(numStr)
			if blockRemainSize != nodeBlockSize[nodeId] {
				//如果block不是空的，应该更换新文件存放分片数据
				num++
				path = str[0] + "." + strconv.Itoa(num)
				bucketInfoSet[nodeId].BlockPath = path
				bucketInfoSet[nodeId].BlockValidSpace = nodeBlockSize[nodeId]
			}

		}
		//分片所存放路径是：节点根目录/bucketId/blockPath
		location.BlockPath = bucketInfoSet[nodeId].BlockPath
		location.OffSet = nodeBlockSize[nodeId] - bucketInfoSet[nodeId].BlockValidSpace + 1
		location.Length = sliceInfo.Length
		sliceLocationSet[location.Num] = &location

		//更新剩余空间大小
		if bucketInfoSet[nodeId].BlockValidSpace <= location.Length {
			bucketInfoSet[nodeId].BlockValidSpace = 0
		} else {
			bucketInfoSet[nodeId].BlockValidSpace -= location.Length
		}
		log.Debug("Slice:%v's LoactionPlan is: %v", i, location)
	}

	log.Info("construct storage plan for:%v success", objId)
	return sliceLocationSet, nil
}

//对象的相关数据记录到数据库，对象上传的最后一步
func (dataHandler *DataHandle) InsertObjectSliceRecord(meta *app.ObjectMetadataUploadReq, sliceLocations map[uint32]*app.ObjectSliceLocation, bucketId string) error {

	var err error
	defer func() {
		log.Info("Insert all data finish")
	}()
	//构造元数据，后续的循环中将存入分片信息
	record := constructMetadataRecord(meta, bucketId)
	record.SliceInfo = make([]*tcaplus_uecqms.Tb_Object_Slice, 0)
	//err = dataHandler.GetTcaplusClient().InsertObjectMetadata(record)
	////插入元数据信息，元数据信息中包含分片信息
	//err = dataHandler.GetTcaplusClient().InsertObjectMetadata(record)
	//if err != nil {
	//	log.Error("Insert object's metadata failed:%v", err)
	//	return fmt.Errorf("Insert object's metadata failed:%v", err)
	//}
	dataHandler.mu.Lock()
	//获取系统中的节点
	allNodeInfo := dataHandler.aliveNodeMap
	dataHandler.mu.Unlock()
	//后期改进：使用事务
	//key是nodeId-bucketId
	bucketInfoInNodeMap := make(map[string]*tcaplus_uecqms.Tb_Bucket_In_Node)
	for _, location := range sliceLocations {
		//构造分片信息，之后将存入元数据中
		sliceRecord := tcaplus_uecqms.NewTb_Object_Slice()
		//sliceRecord := tcaplus_uecqms.NewTb_Slice()
		sliceRecord.ObjectId = location.ObjectId
		sliceRecord.SliceId = location.Num
		sliceRecord.NodeId = location.NodeId
		//类型
		sliceRecord.BlockPath = location.BlockPath
		sliceRecord.Offset = location.OffSet
		sliceRecord.Length = (uint64)(location.Length)
		sliceRecord.Md5 = location.Md5

		//获取Node信息
		nodeRecord, ok := allNodeInfo[sliceRecord.NodeId]
		if !ok {
			//节点在插入之前失效
			log.Error("Query node at InsertObjectSliceRecord of object:%v failed:%v", location.ObjectId, err)
			break
		}
		//记录最大的block.x中的x以及剩余空间

		//根据nodeId和bucketId找到bucket_in_node记录
		bucketInfoInNode := tcaplus_uecqms.NewTb_Bucket_In_Node()
		nodeIdStr := strconv.Itoa((int)(nodeRecord.Id))
		bucketInNodeKey := nodeIdStr + "-" + bucketId
		if _, ok := bucketInfoInNodeMap[bucketInNodeKey]; !ok {

			bucketInfoInNode.NodeId = nodeRecord.Id
			bucketInfoInNode.BucketId = bucketId
			//将该节点下面的bucketId对应的信息获取出来：在构造分片方案时，已经创建出了bucketInfoInNode记录
			//log.Warn("Get bucket in node:nodeId:%v|bucketId:%v", nodeIdStr, bucketId)
			bucketInfoInNode, err = dataHandler.GetTcaplusClient().GetBucketInfoInNode(bucketInfoInNode)
			if err != nil {
				log.Error("Get bucket info in node failed:%v", err)
				break
			}
			bucketInfoInNodeMap[bucketInNodeKey] = bucketInfoInNode
		} else {
			bucketInfoInNode = bucketInfoInNodeMap[bucketInNodeKey]
		}
		if strings.Compare(bucketInfoInNode.BlockPath, sliceRecord.BlockPath) != 0 {
			//更新目录和剩余空间
			//说明创建了新文件
			str := strings.Split(sliceRecord.BlockPath, ".")
			numStr := str[1]
			num, err := strconv.Atoi(numStr)
			if err != nil {
				log.Error("strconv.Atoi(%v) failed:%v", numStr, err)
				break
			}
			curBlockSuffixStr := strings.Split(bucketInfoInNode.BlockPath, ".")
			numSuffixStr := curBlockSuffixStr[1]
			numSuffix, errSuffix := strconv.Atoi(numSuffixStr)
			if errSuffix != nil {
				log.Error("strconv.Atoi(%v) failed:%v", numSuffixStr, errSuffix)
				break
			}
			if numSuffix < num {
				//说明因存放分片数据的需要，创建了新文件
				bucketInfoInNode.BlockPath = sliceRecord.BlockPath
				bucketInfoInNode.BlockValidSpace = nodeRecord.BlockSize
			}
		}
		if bucketInfoInNode.BlockValidSpace <= (uint64)(sliceRecord.Length) {
			bucketInfoInNode.BlockValidSpace = 0
		} else {
			bucketInfoInNode.BlockValidSpace = bucketInfoInNode.BlockValidSpace - (uint64)(sliceRecord.Length)
		}
		bucketInfoInNodeMap[bucketInNodeKey] = bucketInfoInNode
		record.SliceInfo = append(record.SliceInfo, sliceRecord)
	}

	record.SliceCount = (int32)(len(record.SliceInfo))
	//统一更新bucketInNode
	for _, bucketInfoInNode := range bucketInfoInNodeMap {
		errU := dataHandler.GetTcaplusClient().UpdateBucketInfoInNode(bucketInfoInNode)
		if errU != nil {
			err = errU
			log.Error("Update bucket in node:%v failed:%v", bucketInfoInNode, errU)
			break
		}
		log.Debug("Updata bucket in node to:%v", bucketInfoInNode)
	}

	//插入元数据信息，元数据信息中包含分片信息
	err = dataHandler.GetTcaplusClientObject().InsertObjectMetadata(record)
	if err != nil {
		log.Error("Insert object's metadata failed:%v", err)
		return fmt.Errorf("Insert object's metadata failed:%v", err)
	}

	rollback := false
	rollbackFailed := false
	if err != nil {
		rollback = true
		//获取元数据的所有分片信息，一一删除，然后删除元数据

		err = dataHandler.GetTcaplusClientObject().DeleteObjectMetadata(record)
		if err != nil {
			log.Fatal("Rollback failed:%v when delete object:%v's metadata", err, record.ObjectId)
			return fmt.Errorf("Rollback failed:%v when delete object:%v's metadata", err, record.ObjectId)
		}
	}
	if rollbackFailed {
		log.Fatal("Rollback failed:%v when delete object:%v's slice", err, record.ObjectId)
		return fmt.Errorf("Rollback failed:%v when delete object:%v's slice", err, record.ObjectId)
	}
	if rollback {
		log.Error("Has Rollback")
		return fmt.Errorf("has Rollback!")
	} else {
		return nil
	}

	log.Info("Upload object:%v success", record.ObjectId)
	return nil
}

//dispatch获取所有分片的落地结果后，发送给dataHandle，dataHandle对回复进行处理，此为处理函数
//将根据落地结果决定是重新调整分片方案还是回复上传失败
func (dataHandler *DataHandle) SaveObjectRes(ctx context.Context, rsp *app.ObjectSaveRsp) error {

	log.Info("receive save result of object:%v", rsp.ObjectId)
	log.Info("object:%v's slice save result is:%v", rsp.ObjectId, *rsp)
	objId := rsp.ObjectId

	//检查对象的缓存信息是否还存在，不存在则直接丢弃
	dataHandler.mu.Lock()
	_, ok := dataHandler.objectDataInfoMap[objId]
	dataHandler.mu.Unlock()
	if !ok {
		rspToDispatch := &app.ObjectUploadRsp{
			ObjectId: objId,
		}
		rspToDispatch.Ret = base.RET_ERROR
		rspToDispatch.ErrorMsg = "save object failed!"

		dataHandler.ObjectUploadDone(rspToDispatch)
		return nil
	}

	dataHandler.mu.Lock()
	curDataInfo := dataHandler.objectDataInfoMap[objId]
	dataHandler.mu.Unlock()

	curDataInfo.mu.Lock()
	curDataInfo.retryTimes--
	remainTimes := curDataInfo.retryTimes
	curDataInfo.mu.Unlock()
	//向dispatch发送对象落盘完成消息
	rspToDispatch := &app.ObjectUploadRsp{
		ObjectId: objId,
	}
	rspToDispatch.Ret = base.RET_ERROR
	if remainTimes == 0 && rsp.Ret != base.RET_OK {
		//重现调整分片方案的次数用完，回复失败
		rspToDispatch.ErrorMsg = "save object failed!"
		dataHandler.ObjectUploadDone(rspToDispatch)

		//删除元数据和分片记录
		log.Fatal("Confirm object:%v upload result:Error", objId)
		return nil
	}
	if rsp.Ret == base.RET_OK {
		//说明数据全部落盘成功:需要将数据记录到DB中，并告知dispatch成功
		log.Info("all slice of object:%v save success,start save to tcaplus", objId)
		//(2)记录到DB
		userId, bucketName, _ := tcaplus_api.DecodeObjectId(objId)
		bucketId := tcaplus_api.EncodeBucketId(userId, bucketName)
		sliceLocations := curDataInfo.sliceInfo
		metadata := curDataInfo.metaData
		err := dataHandler.InsertObjectSliceRecord(metadata, sliceLocations, bucketId)
		if err != nil {
			log.Error("Insert slice record failed of object%v after all slice save success", objId)
		} else {
			rspToDispatch.Ret = base.RET_OK
		}
		log.Info("Update tcaplus for object%v success!", objId)
		//(3)告知dispatch
		dataHandler.ObjectUploadDone(rspToDispatch)
		log.Debug("Confirm object:%v upload result:OK", objId)
		return nil
	}
	//到此说明有分片落地失败且还可以重新调整落地方案，调整后重新发送

	//重新选择node
	//获取目前所有的存活节点
	dataHandler.mu.Lock()
	nodeSet := dataHandler.aliveNodeMap
	dataHandler.mu.Unlock()
	nodeInfoSet := make([]*tcaplus_uecqms.Tb_Node_Info, 0)
	validNodeCount := 0
	for _, node := range nodeSet {
		if node.State == 0 {
			continue
		}
		validNodeCount++
		nodeInfoSet = append(nodeInfoSet, node)
	}

	plan := &app.ObjectStoragePlanRsp{}
	plan.SliceLocation = make([]app.ObjectSliceLocation, 0)
	for _, sliceRes := range rsp.SliceSaveRes {
		if sliceRes.Ret != base.RET_OK {
			//确定哪个分片在哪个节点的存储失败
			sliceNum := sliceRes.Num
			nodeId := (uint64)(sliceRes.NodeId)
			sliceLocation := curDataInfo.sliceInfo[sliceNum]

			//为其重新选择节点
			for _, node := range nodeSet {
				if node.Id == nodeId {
					continue
				}
				sliceLocation.NodeId = node.Id
				plan.SliceLocation = append(plan.SliceLocation, *sliceLocation)
			}
		}
	}

	if len(plan.SliceLocation) == 0 {
		//告知UploadMetadata
		curDataInfo.stopChan <- (-1)
		return nil
	}
	//发送getdispatch的StoragePlan
	payloadBytes, err := dataHandler.base.GetCodeC().Marshal(reflect.ValueOf(plan))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", plan, err)
		return nil
	}
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("StoragePlan")
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
	//加锁，或者头部和payload一起发送
	session.GetMutex().Lock()
	defer session.GetMutex().Unlock()
	//写入头部
	_, errW := session.GetConnection().Write(headerBytes)
	if errW != nil {
		log.Error("failed to write header with,err is:%v\n", errW)
		return nil
	}
	//写入请求报文
	_, errW = session.GetConnection().Write(payloadBytes)
	if errW != nil {
		log.Error("failed to write req msg,err is:%v\n", errW)
		return nil
	}
	return nil
}

//向dispatch发送分片落地结果
func (dataHandler *DataHandle) ObjectUploadDone(rsp *app.ObjectUploadRsp) error {

	//一旦对dispatch调用该函数进行回复，那么该对象的上传也就结束，无论成功或者失败
	//删除objid到dispatch的session的映射
	defer func() {
		dataHandler.mu.Lock()
		delete(dataHandler.objectDataInfoMap, rsp.ObjectId)
		delete(dataHandler.objectDispatchMap, rsp.ObjectId)
		dataHandler.mu.Unlock()

		//在最终成功之后，更新用户的bucket的对象数量，以及存储总量，对象数量直接获取总数即可，但存储总量要实时变动(上传、删除时)
	}()
	payloadBytes, err := dataHandler.base.GetCodeC().Marshal(reflect.ValueOf(rsp))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *rsp, err)
		return err
	}
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}

	header.SetCmd("ObjectUploadDone")
	//编码头部
	headerBytes, errB := dataHandler.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
	}

	//根据objId找到对应的session
	objId := rsp.ObjectId
	//在dataHandle中选择所连接的diapatch，将其发送出去
	dataHandler.mu.Lock()
	if _, ok := dataHandler.objectDispatchMap[objId]; !ok {
		log.Error("dataHandler.objectDispatchMap[%v] has erased!", objId)
		dataHandler.mu.Unlock()
		return fmt.Errorf("dataHandler.objectDispatchMap[%v] has erased!", objId)
	}
	s := dataHandler.objectDispatchMap[objId]
	dataHandler.mu.Unlock()
	log.Debug("Get session by taskId:%v", objId)
	s.Write(headerBytes, payloadBytes)
	if err != nil {
		log.Warn("Response dispatch failed: %v", err)
	}
	return nil
}

//当需要查询数据时，dispatchSvr向dataHandle请求对象分片的详细信息
func (dataHandler *DataHandle) ObjectInfoQuery(ctx context.Context, req *app.ObjectReq, rsp *app.ObjectLocationRsp) error {

	var err error
	//首先根据userid和bucketName以及objName，查找分片信息

	rsp.Ret = base.RET_ERROR

	userId := req.UserId
	rsp.UserId = userId

	bucketName := req.BucketName
	rsp.BucketName = bucketName

	objName := req.ObjName
	rsp.ObjectName = objName

	objId := tcaplus_api.EncodeObjectId(userId, bucketName, objName)
	bucketId := tcaplus_api.EncodeBucketId(userId, bucketName)

	//首先获取元数据，元数据记录了文件的类型
	metaRecord := tcaplus_uecqms.NewTb_Object_Metadata()
	metaRecord.BucketId = bucketId
	metaRecord.ObjectName = objName
	metaRecord.ObjectId = objId
	metaRecord, err = dataHandler.GetTcaplusClientObject().GetMetadata(metaRecord)
	if err != nil {
		log.Warn("get meta of userId:%v|bucketName:%v|objectName:%v failed", userId, bucketName, objName)
		return err
	}
	//转换为元数据结构
	rsp.ObjMetadata.Name = metaRecord.ObjectName
	rsp.ObjMetadata.ContentLength = metaRecord.ContentLength
	rsp.ObjMetadata.ContentType = metaRecord.ContentType
	rsp.ObjMetadata.ContentEncode = metaRecord.ContentEncode
	rsp.ObjMetadata.Suffix = metaRecord.Suffix
	rsp.ObjMetadata.UploadTime = metaRecord.UploadTime
	rsp.ObjMetadata.Md5 = metaRecord.Md5
	rsp.ObjMetadata.IsEncript = metaRecord.IsEncript
	rsp.ObjMetadata.EncriptAlgo = metaRecord.EncriptAlgo
	rsp.ObjMetadata.Expire = metaRecord.Expire
	rsp.ObjMetadata.OriDataSliceCount = metaRecord.SliceDataCount
	rsp.ObjMetadata.ECCodeSliceCount = metaRecord.SliceECCodeCount

	//取出分片信息，存入回复信息中
	sliceSet := metaRecord.SliceInfo
	rsp.ObjSliceLocation = make([]app.ObjectSliceLocation, 0)
	for _, slice := range sliceSet {
		location := app.ObjectSliceLocation{
			ObjectId:  objId,
			Num:       slice.SliceId,
			NodeId:    slice.NodeId,
			BlockPath: slice.BlockPath,
			OffSet:    slice.Offset,
			Length:    (uint64)(slice.Length),
			Md5:       slice.Md5,
		}
		if slice.Type == 0 {
			location.Type = app.SliceData
		} else {
			location.Type = app.SliceECCode
		}
		rsp.ObjSliceLocation = append(rsp.ObjSliceLocation, location)
	}

	rsp.Ret = base.RET_OK
	return nil

}

//删除对象
func (dataHandler *DataHandle) ObjectDelete(ctx context.Context, req *app.ObjectReq, rsp *app.ObjectDeleteRsp) error {
	var err error

	//首先根据userid和bucketName以及objName，查找分片信息
	rsp.Ret = base.RET_ERROR
	userId := req.UserId
	bucketName := req.BucketName
	bucketId := tcaplus_api.EncodeBucketId(userId, bucketName)
	objName := req.ObjName
	objId := tcaplus_api.EncodeObjectId(userId, bucketName, objName)
	//上报对象删除请求
	go dataHandler.uploadObjectDeleteRequest(ctx, req)

	defer func() {
		//退出时，上报删除结果
		go dataHandler.uploadObjectDeleteResult(ctx, req, rsp.Ret)
	}()
	//应该以事务方式执行

	//先更新bucket中的存储信息

	//删除元数据:先查询出来，再删除
	objRecord := tcaplus_uecqms.NewTb_Object_Metadata()
	objRecord.BucketId = bucketId
	objRecord.ObjectId = objId
	objRecord, err = dataHandler.GetTcaplusClientObject().GetMetadata(objRecord)
	if err != nil {
		log.Error("ObjectDelete failed:%v! There is no object:%v in bucket:%v of user:%v", err, objName, bucketName, userId)
		return fmt.Errorf("ObjectDelete failed!")
	}
	//元数据和分片存放在一起，可一并删除
	err = dataHandler.GetTcaplusClientObject().DeleteObjectMetadata(objRecord)
	if err != nil {
		log.Error("delete object %v failed: %v", objName, err)
		return fmt.Errorf("delete object %v failed: %v", objName, err)
	}
	rsp.Ret = base.RET_OK
	return nil
}

func (dataHandler *DataHandle) eraseDataInfo(objId string) {
	dataHandler.mu.Lock()
	delete(dataHandler.objectDataInfoMap, objId)
	dataHandler.mu.Unlock()
}
