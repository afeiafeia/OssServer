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
	"reflect"
	"time"
)

//dataHandle发送过来的分片落地方案的处理函数
//分片方案是在dispatch收到分片数据后，向dataHandle请求的
func (dispatch *Dispatch) SliceStoragePlan_Old(ctx context.Context, rsp *app.ObjectStoragePlanRsp) error {

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
	curSaveStrategy.mu.Unlock()

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
func (dispatch *Dispatch) UploadSliceData_Old(ctx context.Context, req *app.ObjectSliceUploadReq, rsp *app.ObjectUploadRsp) error {

	dispatch.mu.Lock()
	dispatch.clientReqCount++
	dispatch.mu.Unlock()
	rsp.Ret = base.RET_ERROR
	//记录一下上传请求:上传至influxdb(暂时关闭)
	//go dispatch.UploadObjectSliceRequest(ctx)

	//根据此id也唯一确定一个对象
	objectId := tcaplus_api.EncodeObjectId(req.UserId, req.BucketName, req.ObjName)
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

//向dataHandle请求落地方案
func (dispatch *Dispatch) GetSliceStoragePlan(objId string) error {

	req := &app.ObjectStoragePlanReq{
		ObjectId: objId,
	}
	//构造报文。回复client
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *req, err)
		return fmt.Errorf("Encoding plan: %v failed: %v", *req, err)
	}
	payloadLen := len(payloadBytes)
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("SliceStoragePlan")
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
	}
	//向dataHandle的session发送报文
	successs := false
	for _, session := range dispatch.dataHandleClients {
		//写入头部
		errW := session.Write(headerBytes, payloadBytes)
		if errW != nil {
			log.Error("failed to write header,err is:%v\n", errW)
			continue
		}
		//只需要发往任意一个dataHandle即可
		successs = true
		break
	}
	if !successs {
		log.Error("Send header failed at all dataHandle!")
		return fmt.Errorf("Send header failed")
	}
	return nil
}

//在得到落盘方案后，向node发送分片数据
func (dispatch *Dispatch) saveSliceData(objId string, curSaveStrategy *ObjectSaveStrategy) {

	//获取数据，进入此函数，说明objId对应的分片和数据都已经到达了，
	plan := curSaveStrategy.objectSlicePlan
	data := curSaveStrategy.objectSliceData

	curSaveStrategy.mu.Lock()
	version := curSaveStrategy.version
	curSaveStrategy.mu.Unlock()

	log.Debug("object:%v's strategy is:%v", objId, curSaveStrategy)
	for _, sliceLocation := range plan.SliceLocation {
		nodeId := sliceLocation.NodeId
		//根据落地方案中的nodeId找到session(node的连接)
		dispatch.mu.Lock()
		session := dispatch.nodeSession[nodeId]
		dispatch.mu.Unlock()
		//将数据发往指定node
		objId := sliceLocation.ObjectId
		sliceNum := sliceLocation.Num
		curSaveStrategy.mu.Lock()
		curSaveStrategy.objectSaveResEverySlice[sliceNum] = 0
		curSaveStrategy.mu.Unlock()
		if session == nil {
			//如果对应的节点已经下线，不再发送这一个分片，直接标记为出错，将会由dataHandle调整
			curSaveStrategy.mu.Lock()
			curSaveStrategy.objectSaveResEverySlice[sliceNum] = -1
			curSaveStrategy.mu.Unlock()
			continue
		}
		sliceSave := &app.ObjSliceSaveReq{
			ObjectId:  objId,    //分片所属对象的对象id
			NodeId:    nodeId,   //分片所发往的node
			Num:       sliceNum, //分片的编号
			Version:   version,
			BlockPath: sliceLocation.BlockPath, //分片所存放的地址
			Offset:    sliceLocation.OffSet,
			Data:      data.Slice[sliceNum].Data, //分片的实际数据
		}
		log.Info("Send object:%v's sliceData with length:%v to node%v's file:%v", objId, len(sliceSave.Data), nodeId, sliceLocation.BlockPath)
		go dispatch.sendSliceDataToNode(sliceSave, session)
		log.Debug("Send sliceData with length:%v to node%v's file:%v", len(sliceSave.Data), nodeId, sliceLocation.BlockPath)
	}
}

//汇总并发送分片的落盘结果
func (dispatch *Dispatch) gatherSliceSaveResult(objectId string, curSaveStrategy *ObjectSaveStrategy) {
	//rsp回复给dataHandle
	rsp := &app.ObjectSaveRsp{
		ObjectId: objectId,
	}
	rsp.Ret = base.RET_OK
	rsp.SliceSaveRes = make([]app.ObjSliceSaveRes, 0)
	curSaveStrategy.mu.Lock()
	//检查有哪些分片成功，哪些分片失败，信息发给dataHandle
	for sliceNum, res := range curSaveStrategy.objectSaveResEverySlice {
		if res != 1 {
			//有分片落地失败
			//错误信息记录到rsp中以回复给dataHandle
			curSliceRes := app.ObjSliceSaveRes{
				Ret:    (int)(res),
				Num:    sliceNum,
				NodeId: 1,
			}
			rsp.SliceSaveRes = append(rsp.SliceSaveRes, curSliceRes)
			rsp.Ret = base.RET_ERROR
			log.Error("objectId:%v's Slice:%v save failed", objectId, sliceNum)
		}
	}
	curSaveStrategy.mu.Unlock()
	//向dataHandle发送分片的落地结果
	err := dispatch.sendSliceDataSaveRes(rsp)
	if err != nil {
		//如果发送失败，说明dataHandle断开连接了，直接返回，清除所有记录，直接返回

		//后期可修改为：通过其他DataHandle访问
		return
	}
}

//告知dataHandle分片落地方案的落盘结果
func (dispatch *Dispatch) sendSliceDataSaveRes(res *app.ObjectSaveRsp) error {
	//构造报文。回复client
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(res))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *res, err)
		return fmt.Errorf("Encoding plan: %v failed: %v", *res, err)
	}
	payloadLen := len(payloadBytes)
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("SaveObjectRes")
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
	}
	//向dataHandle的session发送报文
	for _, session := range dispatch.dataHandleClients {
		//写入头部
		errW := session.Write(headerBytes, payloadBytes)
		if errW != nil {
			log.Error("failed to write header,err is:%v\n", errW)
			continue
		}
		break
	}
	log.Info("Notify res:%v to DataHandle!", *res)
	return nil
}

func (dispatch *Dispatch) ObjectUploadDone(ctx context.Context, req *app.ObjectUploadRsp) error {

	//log.Warn("Object upload is finished:%v", *req)
	objectId := req.ObjectId
	//先检查一下对应的对象的上传状态是否还存在，如果不存在，说明对象的上传已经结束了，直接退出
	dispatch.mu.Lock()
	if _, ok := dispatch.objectSaveStrategyMap[objectId]; !ok {
		dispatch.mu.Unlock()
		log.Warn("Object:%v's save has finished(failed)", objectId)
		return nil
	}
	curSaveStrate := dispatch.objectSaveStrategyMap[objectId]
	dispatch.mu.Unlock()
	//如果存在，通知
	if req.Ret != base.RET_OK {
		//通知失败结果
		curSaveStrate.objectSaveFinishChan <- (-1)
		log.Error("Object upload failed:%v", objectId)
	} else {
		//通知成功结果
		curSaveStrate.objectSaveFinishChan <- 1
		//log.Warn("Object upload success:%v", objectId)
	}

	return nil
}

//向node发送存储文件的请求
func (dispatch *Dispatch) sendSliceDataToNode(req *app.ObjSliceSaveReq, session *base.Session) {
	if session == nil {
		return
	}
	offset := (uint64)(req.Offset)
	num := (uint64)(req.Num)
	nodeId := (uint64)(req.NodeId)
	versoin := (uint64)(req.Version)
	taskId := tcaplus_api.EncodeLocationId(offset, num, nodeId, versoin, req.BlockPath, req.ObjectId)
	log.Debug("Taskid is:%v with len:%v", taskId, len(taskId))
	//payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	//if err != nil {
	//	log.Error("Encoding plan: %v failed: %v", *req, err)
	//	return
	//}

	payloadBytes := req.Data
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("SaveObjectSliceData")
	header.SetTaskId(taskId)
	//编码头部
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		return
	}
	//头部和payload一起发送
	err := session.Write(headerBytes, payloadBytes)
	if err != nil {
		log.Warn("Write rsp header failed %v", err)
		return
	}

}

//通过sendSliceDataToNode向node发送数据后，该函数用来处理收到的回复
func (dispatch *Dispatch) SaveObjectSliceData(ctx context.Context, rsp *app.ObjSliceSaveRsp) error {
	log.Debug("receive response from:%v", *rsp)
	objId := rsp.ObjectId
	dispatch.mu.Lock()
	//检查对应的objectSaveStrategy是否还存在：可能存在的情况是：
	//saveSliceData认为超时，已经返回了，将当前分片方案版本的记录清除了
	//此时再收到延迟到达的报文，访问会出错，因此这里直接丢弃
	if _, ok := dispatch.objectSaveStrategyMap[objId]; !ok {
		dispatch.mu.Unlock()
		log.Error("object:%v is not exist!", objId)
		return nil
	}
	//找出对应的ObjectSaveStrategy
	curSaveStrategy := dispatch.objectSaveStrategyMap[objId]
	dispatch.mu.Unlock()
	//返回报文的节点的nodeId
	sliceNum := rsp.Num
	curSaveStrategy.mu.Lock()
	//检查一下版本是否一致，可能在重传之后，原方案的落地结果又延迟到达了
	version := curSaveStrategy.version
	if version != rsp.Version {
		curSaveStrategy.mu.Unlock()
		log.Warn("Timeout message!")
		return nil
	}
	if rsp.Ret == base.RET_OK {
		//分片sliceNum的结果:落盘成功
		curSaveStrategy.objectSaveResEverySlice[sliceNum] = 1
	} else {
		//分片sliceNum的结果：落盘失败
		curSaveStrategy.objectSaveResEverySlice[sliceNum] = -1
	}
	curSaveStrategy.objectSliceCount--
	if curSaveStrategy.objectSliceCount == 0 {
		//减为0说明所有分片的落盘都已经有了一个结果
		//通知saveSliceData
		log.Debug("All slice has result!")
		curSaveStrategy.objectSaveResChan <- 1
	}
	curSaveStrategy.mu.Unlock()
	return nil
}

//client发送过来的请求：请求获取对象
//处理逻辑：
//(1)编码出所请求对象的唯一id：objectId
//(2)根据唯一id，记录id到对应client的session的映射
//(3)将请求转发给dataHandle
//(4)等待元数据的请求结果，如果请求失败，回复error,返回，如果成功，回复ok，并附带上元数据，继续执行下一步
//(5)根据收到的分片方案，向节点请求实际分片数据
func (dispatch *Dispatch) ObjectQuery(ctx context.Context, req *app.ObjectReq) error {

	////对查询请求进行上报，上报至influxdb(暂时关闭)
	//go dispatch.UploadObjectQueryRequest(ctx, req)

	rsp := &app.ObjectQueryRsp{
		Ret:      base.RET_ERROR,
		DataType: app.ObjectMetaData,
	}

	//queryRes := base.RET_ERROR
	//首先根据userid和bucketName以及objName，查找分片信息
	userId := req.UserId
	bucketName := req.BucketName
	objName := req.ObjName
	objId := tcaplus_api.EncodeObjectId(userId, bucketName, objName)
	//结束之前上报结果(暂时关闭)
	//defer func() {
	//	//objectLength := (int)(rsp.Meta.ContentLength)
	//	//go dispatch.UploadObjectQueryResult(objId, objectLength, queryRes)
	//}()
	//创建对象记录原数据信息，在该函数退出时，将其删除，同时将objectId->client's session的映射删除
	s := ctx.Value("session")
	session := s.(*base.Session)
	dispatch.base.AddTaskSession(objId, session)
	log.Debug("bind objectId:%v to client session", objId)

	dispatch.mu.Lock()
	if _, ok := dispatch.objectQueryResMap[objId]; !ok {
		//说明该对象正在查询处理中，停止其它查询
		dispatch.objectQueryResMap[objId] = new(ObjectQueryRes)
		dispatch.objectQueryResMap[objId].objectMetadataQueryResChan = make(chan int8, 1)
	}
	curQueryRes := dispatch.objectQueryResMap[objId]
	dispatch.mu.Unlock()

	defer func() {
		//该函数退出时，表示objId对应对象的请求的结束，删除暂存的数据信息
		//该对象的获取流程结束，清除对该对象信息的记录
		dispatch.mu.Lock()
		delete(dispatch.objectQueryResMap, objId)
		dispatch.mu.Unlock()
	}()

	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *req, err)
		rsp.ErrorMsg = "error in server!"
		dispatch.sendMetadataToClient(ctx, objId, rsp)
		return nil
	}
	payloadLen := len(payloadBytes)
	//向dataHandle发送获取分片数据的请求
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("ObjectInfoQuery")
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		rsp.ErrorMsg = "error in server!"
		dispatch.sendMetadataToClient(ctx, objId, rsp)
		return nil
	}
	//向dataHandle请求对象的元数据和落地方案
	//因为是从数据库中查询，所以可以发送给其所连接的任一个dataHandle
	log.Debug("request metadata of object:%v", objId)
	for _, s := range dispatch.dataHandleClients {
		err = s.Write(headerBytes, payloadBytes)
		if err != nil {
			continue
		}
	}
	if err != nil {
		log.Error("Requert metadata failed%v\n", err)
		rsp.ErrorMsg = "error in server!"
		dispatch.sendMetadataToClient(ctx, objId, rsp)
		return nil
	}
	//创建管道:等待dataHandle发送过来回复
	//此处等待请求的结果
	metaReqTimeOutChan := time.After(5 * time.Second)
	var res int8
	res = 0
	select {
	case <-metaReqTimeOutChan:
		log.Warn("create bucket time out")
		rsp.ErrorMsg = "time out"
		return nil
	case res = <-curQueryRes.objectMetadataQueryResChan:
		//所有分片都得到了处理
		log.Debug("receive metadata and slicelocation")
	}
	if res == 1 {
		//对象元数据获取成功，数据被记录到了curQueryRes的sliceInfo中
		//获取成功,填入元数据信息
		curQueryRes.mu.Lock()
		rsp.Meta = curQueryRes.sliceInfo.ObjMetadata
		curQueryRes.mu.Unlock()

	} else {
		//元数据获取失败
		log.Error("Query metadata of objectId:%v failed", objId)
		rsp.ErrorMsg = "error in server!"
		return nil
	}
	//将元数据信息发送给client
	dispatch.sendMetadataToClient(ctx, objId, rsp)

	//根据分片的落地方案，向node请求分片数据
	dispatch.querySliceDataFromNode(ctx, curQueryRes)
	//继续等待处理结果
	sliceResTimeOutChan := time.After(10 * time.Second)
	res = 0
	select {
	case <-sliceResTimeOutChan:
		log.Warn("Query slice data time out")
		return nil
	case res = <-curQueryRes.objectQueryResChan:
		//所有分片都得到了处理
		log.Info("all slice has result")
	}
	if res == 1 {
		log.Info("all slice has success result")
		//获取成功

	} else {
		log.Info("some slice query failed")
		//获取失败：超时或者失败
	}
	return err
}

//经上面的ObjectQuery向dataHandle请求对象的元数据以及落地方案后，获取datahandle发送过来的对象元数据及分片信息
//对于元数据：为什么没有直接转发给clien，而是要dispatch解析出来：因为元数据中记录了分片实际数据、EC码数据的数量，dispatch要用来决定向节点的获取策略
/*
type ObjectLocationRsp struct {
	Ret              int                   `json:"ret"`          //回复状态码
	ErrorMsg         string                `json:"errorMsg"`     //具体错误信息
	UserId           string                `json:"userId"`       //用户id
	BucketName       string                `json:"bucketName"`   //桶名称
	ObjectName       string                `json:"objName"`      //对象名称
	ObjMetadata      ObjMetadata           `json:"objMetaData"`  //对象的元数据信息
	ObjSliceLocation []ObjectSliceLocation `json:"objSliceData"` //对象的分片落地方案
}
*/
func (dispatch *Dispatch) ObjectInfoQuery(ctx context.Context, rsp *app.ObjectLocationRsp) error {
	userId := rsp.UserId
	bucketName := rsp.BucketName
	objectName := rsp.ObjectName
	objId := tcaplus_api.EncodeObjectId(userId, bucketName, objectName)

	//先检查一下是否objId对应的信息仍存在
	dispatch.mu.Lock()
	//可能存在的情况时：由于网络延迟，对于对象的获取超时，获取失败，但之后node发送过来了分片数据
	//此处检查一下是否状态结束，如果已经结束，直接丢弃报文
	if _, ok := dispatch.objectQueryResMap[objId]; !ok {
		dispatch.mu.Unlock()
		return nil
	}
	queryRes := dispatch.objectQueryResMap[objId]
	dispatch.mu.Unlock()
	queryRes.mu.Lock()

	//如果仍存在，进行后续处理：通知主协程
	if rsp.Ret == base.RET_OK {
		//获取成功
		oriDataSliceCount := rsp.ObjMetadata.OriDataSliceCount
		log.Debug("OriginDataCount is:%v", oriDataSliceCount)
		eccodeSliceCount := rsp.ObjMetadata.ECCodeSliceCount
		log.Debug("ECCodeDataCount is:%v", eccodeSliceCount)

		sliceCount := oriDataSliceCount + eccodeSliceCount
		queryRes.sliceInfo = rsp
		//通知channel,在收到所有分片的查询结果后，写入信息，该协程select将收到通知
		queryRes.objectQueryResChan = make(chan int8, sliceCount)
		//总分片的数量
		queryRes.objectSliceCount = sliceCount
		//当有分片获取失败时，下一个可以获取的分片，在curQueryRes.sliceInfo的ObjSliceLocation中的下标
		queryRes.objectSliceNextIndex = oriDataSliceCount
		//对象所有分片中，属于原始数据的分片的数量，0-oriDataSliceCount-1的下标中是原始分片
		//该值减为0时，表示客户端收到了oriDataSliceCount份分片，就可以进行原文件的恢复
		queryRes.objectOriDataSliceCount = oriDataSliceCount
		//通知ObjectQuery

		queryRes.objectMetadataQueryResChan <- 1

	} else {
		//获取失败
		queryRes.objectMetadataQueryResChan <- (-1)
	}
	queryRes.mu.Unlock()
	return nil
}

func (dispatch *Dispatch) querySliceDataFromNode(ctx context.Context, curQueryRes *ObjectQueryRes) {

	curQueryRes.mu.Lock()
	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	header.SetCmd("ObjectDataQuery")
	simpleHeader := *header
	curQueryRes.mu.Unlock()
	log.Debug("query slice from node:%v", *curQueryRes)
	//当前对象的实际数据分片的数量
	oriDataSliceCount := curQueryRes.objectOriDataSliceCount
	//对象分片的总数量
	curQueryRes.mu.Lock()
	sliceCount := curQueryRes.objectSliceCount
	//当出现分片获取失败时，下一个可以获取的分片在curQueryRes.sliceInfo.ObjSliceLocation中的下标
	j := curQueryRes.objectSliceNextIndex
	curQueryRes.mu.Unlock()

	dispatch.mu.Lock()
	nodeSession := dispatch.nodeSession
	dispatch.mu.Unlock()
	//发送给相应节点
	for i := 0; i < (int)(oriDataSliceCount); i++ {
		sliceInfo := curQueryRes.sliceInfo.ObjSliceLocation[i]
		log.Debug("i:%v's sliceInfo is:%v", i, sliceInfo)
		nodeId := sliceInfo.NodeId
		oldId := nodeId
		//先检查节点是否存活：如果不再存活，需要转而获取一份EC码数据
		if _, ok := nodeSession[nodeId]; !ok {
			log.Warn("NodeId:%v is down,need other node", nodeId)
			//转而获取EC码
			for j < sliceCount {
				curQueryRes.mu.Lock()
				curQueryRes.objectSliceNextIndex++
				curQueryRes.mu.Unlock()
				sliceInfo = curQueryRes.sliceInfo.ObjSliceLocation[j]
				j++
				nodeId = sliceInfo.NodeId
				//当前系统中，nodeSession在启动时建立，不需要加锁，但后期，需要动态扩展节点，会对nodeSession进行添加，存在并发问题，所以此处加锁
				//后期优化思路：直接dispatch基本加锁，锁粒度比较大，后期可独立使用一个node的mutex
				_, ok := nodeSession[nodeId]
				if !ok {
					//节点不存在，继续查找
					continue
				}
				log.Info("Forward req of ndoeId:%v to nodeId:%v", oldId, nodeId)
				break
			}
		}

		//直接将sliceInfo发送给指定节点
		session, ok := nodeSession[nodeId]
		if !ok {
			log.Error("There is no node with id:%v online", nodeId)
			break
		}
		log.Debug("Send query request to node:%v", nodeId)
		go dispatch.getDataFromNode(simpleHeader, session, &sliceInfo, curQueryRes)
	}
}

//收到node发送回来的数据后，由该函数进行处理
//node在获取数据后，将实际数据经dispatch直接转发给client；
//另外会再告知dispatch数据获取的结果：如果获取失败，不会有数据转发给client，rsp.Ret是error,如果获取成功，rsp.Ret是ok，同时会有数据经由dispatch被转发给相应的client
//失败情况下，此函数判断哪个分片获取失败，然后转而请求EC码数据
func (dispatch *Dispatch) ObjectDataQuery(ctx context.Context, rsp *app.ObjSliceGetRsp) error {
	objId := rsp.ObjectId
	dispatch.mu.Lock()
	//可能存在的情况时：由于网络延迟，对于对象的获取超时，获取失败，但之后node发送过来了分片数据
	//此处检查一下是否状态结束，如果已经结束，直接丢弃报文
	if _, ok := dispatch.objectQueryResMap[objId]; !ok {
		dispatch.mu.Unlock()
		return nil
	}
	errorNodeId := rsp.NodeId
	queryRes := dispatch.objectQueryResMap[objId]
	dispatch.mu.Unlock()
	if rsp.Ret == base.RET_ERROR {
		//出现数据分片获取失败的情况：需要获取下一个分片
		//下一个可以获取的分片的下标:queryRes.objectSliceNextIndex
		log.Warn("Slice:%v of objectId:%v at nodeId:%v query failed:%v", rsp.Num, rsp.ObjectId, rsp.NodeId, rsp.ErrorMsg)
		var sliceInfo app.ObjectSliceLocation
		var s *base.Session
		queryRes.mu.Lock()
		for i := queryRes.objectSliceNextIndex; i < queryRes.objectSliceCount; i++ {
			//下一个可以获取的分片的信息
			sliceInfo = queryRes.sliceInfo.ObjSliceLocation[i]
			nodeId := sliceInfo.NodeId
			if nodeId == errorNodeId {
				continue
			}
			dispatch.mu.Lock()
			_, ok := dispatch.nodeSession[nodeId]
			s = dispatch.nodeSession[nodeId]
			dispatch.mu.Unlock()
			if !ok {
				continue
			}
		}
		if s == nil {
			//说明所有节点下线，通知主协程结束
			queryRes.objectQueryResChan <- (-1)
			close(queryRes.objectQueryResChan)
			queryRes.mu.Unlock()
			return nil
		}
		queryRes.mu.Unlock()
		h := ctx.Value(base.HEADER)
		header := h.(*codec.SimpleHeader)
		simpleHeader := *header
		go dispatch.getDataFromNode(simpleHeader, s, &sliceInfo, queryRes)

	} else {
		queryRes.mu.Lock()
		//该值减为0时，表示客户端收到了oriDataSliceCount份分片(m+n份分片中的m份)，就可以进行原文件的恢复
		queryRes.objectOriDataSliceCount--
		if queryRes.objectOriDataSliceCount == 0 {
			//所有的分片都成功获取：写入1表示成功
			queryRes.objectQueryResChan <- 1
			close(queryRes.objectQueryResChan)
		}
		queryRes.mu.Unlock()

		//此处将对象查询成功的结果上报至influxdb
	}
	return nil
}

func (dispatch *Dispatch) sendMetadataToClient(ctx context.Context, objId string, req *app.ObjectQueryRsp) error {

	h := ctx.Value(base.HEADER)
	header := h.(*codec.SimpleHeader)
	session := dispatch.base.GetTaskSession(objId)
	if session == nil {
		log.Error("there is no task session bind to:%v", objId)
	}
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *req, err)
		return err
	}
	payloadLen := len(payloadBytes)
	//向dataHandle发送获取分片数据的请求
	header.SetPayloadLength((int32)(payloadLen))
	header.SetCmd("ObjectDataQuery")
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed:%v", errB)
		return fmt.Errorf("GetHeaderBytes failed%v", errB)
	}
	//因为是从数据库中查询，所以可以发送给其所连接的任一个dataHandle
	err = session.Write(headerBytes, payloadBytes)
	if err != nil {
		log.Error("Write to session failed:%v", err)
		return fmt.Errorf("Write to session failed:%v", err)
	}

	return nil
}

func (dispatch *Dispatch) getDataFromNode(header codec.SimpleHeader, s *base.Session, req *app.ObjectSliceLocation, curQueryRes *ObjectQueryRes) {
	payloadBytes, err := dispatch.base.GetCodeC().Marshal(reflect.ValueOf(req))
	if err != nil {
		log.Error("Encoding plan: %v failed: %v", *req, err)
		return
	}
	payloadLen := len(payloadBytes)
	//向dataHandle发送获取分片数据的请求
	header.SetPayloadLength((int32)(payloadLen))
	headerBytes, errB := dispatch.base.GetCodeC().GetHeaderBytes(&header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		return
	}
	//因为是从数据库中查询，所以可以发送给其所连接的任一个dataHandle
	log.Debug("Write Req:getDataFromNode")
	err = s.Write(headerBytes, payloadBytes)
	if err != nil {
		log.Warn("Write Req:getDataFromNode failed%v\n", err)

		//调用ObjectDataQuery发送对下一个分片的请求
		rsp := app.ObjSliceGetRsp{}
		rsp.ObjectId = req.ObjectId
		rsp.Ret = base.RET_ERROR
		rsp.NodeId = req.NodeId
		rsp.ErrorMsg = "Write to node failed!"
		ctx := context.WithValue(context.Background(), base.HEADER, header)
		dispatch.ObjectDataQuery(ctx, &rsp)
		return
	}

}
