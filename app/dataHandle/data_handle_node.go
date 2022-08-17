package dataHandle

import (
	"context"
	"fmt"
	"oss/app"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"strconv"
	"strings"
)

const (
	NODE = "OSS_NODE"
)

func (dataHandler *DataHandle) AddNodeInfo(ctx context.Context, req *app.NodeInfoUpdateReq, rsp *app.NodeBriefInfoRsp) error {

	log.Debug("Get request msg: %v", *req)
	rsp.Ret = base.RET_ERROR
	//先查询所有的节点
	nodeInfoSet, err := dataHandler.GetTcaplusClient().GetAllNode()
	if err != nil {
		log.Error("get all node failed!")
		return fmt.Errorf("get all node failed: %v", err)
	}
	//记录ip+port到id的映射
	aliveNodeMap := make(map[uint64]*tcaplus_uecqms.Tb_Node_Info)
	nodeInfoMap := make(map[string]*tcaplus_uecqms.Tb_Node_Info)
	maxId := (uint64)(0)
	for i, node := range nodeInfoSet {
		key := node.Ip + strconv.Itoa((int)(node.Port))
		//log.Debug("node info is %v", node)
		nodeInfoMap[key] = &nodeInfoSet[i]
		if node.Id > maxId {
			maxId = node.Id
		}
		//如果节点是存活的，记录到dataHandle的aliceNodeMap中
		aliveNodeMap[node.Id] = &nodeInfoSet[i]
		//将节点状态标记为下线
		aliveNodeMap[node.Id].State = 0
		//log.Debug("nodeInfoMap[%v] is %v", key, node)
	}
	dataHandler.mu.Lock()
	if len(dataHandler.aliveNodeMap) != 0 {
		log.Debug("dataHandler.aliveNodeMap is not empty")
		//说明是系统启动之后，添加dataSaveSvr
		aliveNodeMap = dataHandler.aliveNodeMap
	} else {
		//系统启动时的同步
		log.Debug("dataHandler.aliveNodeMap is empty")
	}
	dataHandler.mu.Unlock()
	//for key, node := range nodeInfoMap {
	//	log.Debug("nodeInfoMap[%v] is %v", key, node)
	//}
	log.Debug("All node is:%v", nodeInfoSet)
	rsp.NodeBriefSet = make([]app.NodeBrief, 0)
	for _, node := range req.Node {
		brief := app.NodeBrief{
			Ret: base.RET_OK,
		}
		key := node.Ip + strconv.Itoa((int)(node.Port))
		var nodeRecord *tcaplus_uecqms.Tb_Node_Info
		if _, ok := nodeInfoMap[key]; ok {
			//log.Debug("key is %v", key)
			//log.Debug("node in map is %v", *nodeInfoMap[key])
			log.Debug("Update node info from: %v to: %v", *nodeInfoMap[key], node)
			//如果节点是存在的，更新其状态
			nodeRecord = nodeInfoMap[key]
			nodeRecord.State = 1
			if strings.Compare(nodeRecord.Root, node.Root) != 0 {
				//如果所给的节点的根目录与之前tcaplus中记录的不一致，更改tcaplus中记录的节点的目录信息
				nodeRecord.Root = node.Root
				nodeRecord.Space = node.Space
				nodeRecord.ValidSpace = node.Space
				nodeRecord.BlockSize = node.BlockSize
			}
			//更新到tcaplus中
			err := dataHandler.GetTcaplusClient().UpdateNode(nodeRecord)
			if err != nil {
				log.Error("Update node: %v failed: %v", *nodeRecord, err)
				brief.Ret = base.RET_ERROR
				//return fmt.Errorf("Update node: %v failed: %v", *nodeRecord, err)
			}

		} else {
			log.Debug("Insert node info: %v", node)
			//上报的节点是之前不存在的，重新设置id
			nodeRecord = tcaplus_uecqms.NewTb_Node_Info()
			nodeRecord.Node = NODE
			maxId++
			nodeRecord.Id = maxId
			nodeRecord.Ip = node.Ip
			nodeRecord.Port = node.Port
			nodeRecord.Root = node.Root
			nodeRecord.Space = node.Space
			nodeRecord.ValidSpace = node.Space
			nodeRecord.BlockSize = node.BlockSize
			nodeRecord.State = 1
			err := dataHandler.GetTcaplusClient().InsertNode(nodeRecord)
			if err != nil {
				log.Error("Insert new node: %v failed: %v", *nodeRecord, err)
				brief.Ret = base.RET_ERROR
				//return fmt.Errorf("Insert new node: %v failed: %v", *nodeRecord, err)
			}
		}

		brief.Id = nodeRecord.Id
		brief.Ip = nodeRecord.Ip
		brief.Port = nodeRecord.Port
		rsp.NodeBriefSet = append(rsp.NodeBriefSet, brief)
		//上报上来的节点是存活的，记录到dataHandle中
		if brief.Ret == base.RET_OK {
			aliveNodeMap[nodeRecord.Id] = nodeRecord
		}
	}

	//每次AddNode后，刷新一下dataHandle中存储的节点信息
	dataHandler.mu.Lock()
	for id, _ := range aliveNodeMap {
		dataHandler.aliveNodeMap[id] = aliveNodeMap[id]
	}
	dataHandler.aliveNodeMap = aliveNodeMap
	dataHandler.mu.Unlock()

	rsp.Ret = base.RET_OK
	return nil
}

func (dataHandler *DataHandle) DisableNode(ctx context.Context, req *app.NodeDisable) error {

	log.Debug("DisableNode:%v", req.Id)
	nodeId := req.Id
	dataHandler.mu.Lock()
	//如果节点存在，将状态设置为0，表示下线
	if _, ok := dataHandler.aliveNodeMap[nodeId]; ok {
		log.Debug("Disable session with id:%v", nodeId)
		dataHandler.aliveNodeMap[nodeId].State = 0
	}
	dataHandler.mu.Unlock()

	return nil
}
