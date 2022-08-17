package dispatch

import (
	"context"
	"fmt"
	"oss/app"
	"oss/codec"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"sort"
	"time"
)

//分片方案是在dispatch收到分片数据后，向dataHandle请求的
func (dispatch *Dispatch) SliceStoragePlan(ctx context.Context, rsp *app.ObjectStoragePlanRsp) error {

	//以用户名、桶名称、对象名称确定一个id
	id := tcaplus_api.EncodeObjectId(rsp.UserId, rsp.BucketName, rsp.ObjName)
	log.Info("Receive object:%v's storage plan from dataHandle", id)

	//先检查一下该id对应的对象的记录是否已经被删除，如果是，说明这个是超时到达的回复，直接退出
	dispatch.mu.Lock()
	if _, ok := dispatch.objectSaveStrategyMap[id]; !ok {
		dispatch.mu.Unlock()
		log.Warn("This is timeout response of object:%v!", id)
		return nil
	}
	//如果是正常到达的，取出当前落地方案的落地策略
	curSaveStrategy := dispatch.objectSaveStrategyMap[id]
	dispatch.mu.Unlock()
	//分片数据一定先于落地方案到达

	//记录objId
	curSaveStrategy.mu.Lock()
	//判断一下是不是延迟到达的包
	if curSaveStrategy.objectSlicePlan != nil {
		log.Warn("Repeat strategy plan of object:%v", id)
		curSaveStrategy.mu.Unlock()
		return fmt.Errorf("Repeat strategy plan of object")
	}
	//分片方案的版本
	curSaveStrategy.version++
	//对象id
	curSaveStrategy.objectId = id
	//记录分片落地方案
	curSaveStrategy.objectSlicePlan = rsp
	objData := *curSaveStrategy.tmpObjData
	curSaveStrategy.mu.Unlock()

	//排序
	sliceLocation := rsp.SliceLocation
	sort.Slice(sliceLocation, func(i, j int) bool {
		return sliceLocation[i].Num < sliceLocation[j].Num
	})
	count := len(sliceLocation)
	stIndex := 0
	edIndex := 0
	curSaveStrategy.mu.Lock()
	for i := 0; i < count; i++ {
		objSlice := app.ObjSlice{
			Type: sliceLocation[i].Type,
			Num:  sliceLocation[i].Num,
		}
		edIndex = stIndex + (int)(sliceLocation[i].Length)
		objSlice.Data = make([]byte, sliceLocation[i].Length)
		copy(objSlice.Data, objData[stIndex:edIndex])
		stIndex = edIndex
		curSaveStrategy.objectSliceData.Slice[objSlice.Num] = objSlice
	}
	curSaveStrategy.mu.Unlock()
	log.Info("Construct curSaveStrategy.objectSliceData:ObjectSliceUploadReq")
	//通知UploadSliceData
	if rsp.Ret != base.RET_OK {
		//通知UploadSliceData，分片落地方案获取失败
		curSaveStrategy.objectSlicePlanChan <- (-1)
	} else {
		//通知UploadSliceData，分片落地方案获取成功
		curSaveStrategy.objectSlicePlanChan <- 1
	}

	return nil
}

//当收到client发送过来的分片数据后，暂存在本地，向dataHandle请求分片落地方案
func (dispatch *Dispatch) UploadSliceData(ctx context.Context, objectData *[]byte, rsp *app.ObjectUploadRsp) error {

	log.Info("New UploadSliceData!")
	dispatch.mu.Lock()
	dispatch.clientReqCount++
	dispatch.mu.Unlock()
	rsp.Ret = base.RET_ERROR
	//记录一下上传请求:上传至influxdb(暂时关闭)
	//go dispatch.UploadObjectSliceRequest(ctx)

	//头部的taskId中记录了objectId
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	taskId := header.GetTaskId()
	//根据此id也唯一确定一个对象
	objectId := taskId
	//log.Warn("Start upload object:%v", objectId)
	rsp.ObjectId = objectId
	defer func() {
		//删除分片记录
		dispatch.mu.Lock()
		delete(dispatch.objectSaveStrategyMap, objectId)
		if rsp.Ret == base.RET_OK {
			dispatch.clientRspSuccessCount++
		}
		dispatch.clientRspCount++
		dispatch.mu.Unlock()

		//返回之前将请求的处理结果上报至influxdb(暂时关闭)
		//go dispatch.UploadObjectSliceResult(ctx, rsp)
	}()
	//
	dispatch.mu.Lock()
	curSaveStrategy, ok := dispatch.objectSaveStrategyMap[objectId]
	if !ok {
		dispatch.objectSaveStrategyMap[objectId] = new(ObjectSaveStrategy)
		curSaveStrategy = dispatch.objectSaveStrategyMap[objectId]
	} else {
		//如果是由于网络延迟重复到达，直接丢弃
		log.Warn("Repeat Object:%v's slice data", objectId)
		dispatch.mu.Unlock()
		return fmt.Errorf("Repeat Object's slice data")
	}
	dispatch.mu.Unlock()
	//初始化
	//记录分片数据
	userId, bucketName, objName := tcaplus_api.DecodeObjectId(objectId)
	req := &app.ObjectSliceUploadReq{
		UserId:     userId,
		BucketName: bucketName,
		ObjName:    objName,
	}
	req.Slice = make(map[uint32]app.ObjSlice)
	curSaveStrategy.tmpObjData = objectData
	curSaveStrategy.objectSliceData = req
	//创建管道用于接收分片落地方案由dataHandle发送过来时的通知
	curSaveStrategy.objectSlicePlanChan = make(chan int8, 1)
	//创建管道，用来接收落地完成(可能成功或者失败)后dataHandle的通知
	curSaveStrategy.objectSaveFinishChan = make(chan int8, 1)

	//监视两个管道
	slicePlanChan := curSaveStrategy.objectSlicePlanChan
	finishChan := curSaveStrategy.objectSaveFinishChan
	//向dataHandle请求objectId对应的落地方案
	err := dispatch.GetSliceStoragePlan(objectId)
	if err != nil {
		//这里如果出错，应该删除分片dispatch.sliceData中的记录，然后告知客户端
		log.Error("Get slice storage plan failed!")
		return fmt.Errorf("Get slice storage plan failed!")
	}
	log.Debug("Request object:%v's slice data from datahandle", objectId)

	//此处等待落地方案的回复，直至超时或者收到响应
	//根据响应结果，决定下一步，失败，则直接向client回复失败，
	//如果顺利拿到落地方案，进行下一步
	//利用分片数据和分片落地方案组装发送
	for {
		timeOutChan := time.After(10 * time.Second)
		select {
		case <-timeOutChan:
			log.Warn("receive object:%v's slice plan result from dataHandle time out", objectId)
			return nil
		case planRes := <-slicePlanChan:
			//表示收到分片落地方案
			log.Info("receive storage plan from dataHandle:%v", objectId)
			//收到了分片落地方案,检查
			if planRes == -1 {
				//获取分片出错，返回
				rsp.ErrorMsg = "Error in server"
				return nil
			}
		case finishRes := <-finishChan:
			//收到最终的对象存储结果，进入此，函数将返回，并根据finishRes的值决定回复成功还是失败
			if finishRes == 1 {
				//成功
				rsp.Ret = base.RET_OK
				log.Warn("object upload success:%v", objectId)
			}
			return nil
		}

		//到此说明收到了分片落地方案，将向dataSaveSvr分发数据分片
		//记录要落盘的分片的数量:要落盘的分片数量，由落盘方案决定
		curSaveStrategy.objectSliceCount = (uint32)(len(curSaveStrategy.objectSlicePlan.SliceLocation))
		//用于记录每个分片的结果，由其他函数更改其值
		curSaveStrategy.objectSaveResEverySlice = make(map[uint32]int8)
		//用于在objId的所有落盘都有结果时，通知主协程，每个分片通过协程发送出去，该函数作为主协程等待SaveObjectSliceData对于回复的处理
		curSaveStrategy.objectSaveResChan = make(chan int8, curSaveStrategy.objectSliceCount)
		//该管道用于在所有分片都有结果后通知当前协程
		sliceSaveResChan := curSaveStrategy.objectSaveResChan
		//向node分发数据分片
		go dispatch.saveSliceData(objectId, curSaveStrategy)

		//分发之后，等待dataSaveSvr落地方案的回复
		timeOutChan = time.After(10 * time.Second)
		select {
		case <-timeOutChan:
			log.Error("receive object:%v's slice save result from node time out", objectId)
			return nil
		case <-sliceSaveResChan:
			log.Warn("all slice of object:%v has result", objectId)
			//收到了分片在dataSaveSvr落盘的结果
		}
		//进行落地结果的整理并发送给dataHandle
		//向dataHandle发送分片落地结果，
		curSaveStrategy.mu.Lock()
		curSaveStrategy.objectSlicePlan = nil
		curSaveStrategy.mu.Unlock()
		go dispatch.gatherSliceSaveResult(objectId, curSaveStrategy)
	}
	log.Debug("Storage slice data success")
	return nil
}
