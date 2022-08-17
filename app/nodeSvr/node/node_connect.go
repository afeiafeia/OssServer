package node

import (
	"context"
	"fmt"
	"net"
	"oss/app"
	"oss/codec"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

func (node *NodeSvr) ConnectToOssService(ctx context.Context, rsp *app.NodeBrief) error {

	if rsp.Ret != base.RET_OK {
		//节点接入系统失败
		node.connectFlagChan <- (-1)
		log.Error("Node connect oss service fail")
		return nil
	}
	node.connectFlagChan <- 1
	node.id = rsp.Id
	return nil
}

func (node *NodeSvr) ConnectToDispatchSvr(dispatchInfo *app.DispatchSvrInfo) error {

	connector, err := net.Dial("tcp", dispatchInfo.Ip)
	if err != nil {
		//log.Warn("Connect to diuspatch failed:%v", err)
		return fmt.Errorf("Connect to diuspatch failed:%v", err)
	}
	//对于node的连接，其session的index是node的id，将记录到tcaplus中
	key := dispatchInfo.Ip
	session := base.CreateSession(node.base, &connector, base.CLIENT_SESSION, key)
	log.Debug("Create session:%v by index: %v", session, key)
	//base将开始监视session上的到达请求
	node.base.AddSession(session)
	space, errC1 := strconv.Atoi(dispatchInfo.Space)
	if errC1 != nil {
		log.Error("strconv.Atoi(%v) failed:%v", dispatchInfo.Space, errC1)
		return fmt.Errorf("strconv.Atoi(%v) failed:%v", dispatchInfo.Space, errC1)
	}
	blockSize, errC2 := strconv.Atoi(dispatchInfo.BlockSize)
	if errC2 != nil {
		log.Error("strconv.Atoi(%v) failed:%v", dispatchInfo.BlockSize, errC2)
		return fmt.Errorf("strconv.Atoi(%v) failed:%v", dispatchInfo.BlockSize, errC2)
	}
	//获取系统的ip和端口号
	nodeInfo := &app.NodeInfo{
		Root:      dispatchInfo.Root,
		Space:     TransToByteUnit((uint64)(space), dispatchInfo.SpaceUnit),
		BlockSize: TransToByteUnit((uint64)(blockSize), dispatchInfo.BlockUnit),
	}
	ipInfo := viper.GetString("SERVER_BASE.IPaddr")
	Str := strings.Split(ipInfo, ":")
	portStr := Str[1]
	port, errC := strconv.Atoi(portStr)
	if errC != nil {
		log.Error("strconv.Atoi(%v) failed:%v", portStr, errC)
		return fmt.Errorf("strconv.Atoi(%v) failed:%v", portStr, errC)
	}
	nodeInfo.Port = (uint32)(port)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Error("Get ip failed:%v", err)
		return fmt.Errorf("Get ip failed:%v", err)
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				nodeInfo.Ip = ipnet.IP.String()
				break
			}
		}
	}
	if strings.Compare(nodeInfo.Ip, "") == 0 {
		log.Error("Get ip failed:%v", err)
		return fmt.Errorf("Get ip failed:%v", err)
	}
	//构造头部
	payloadBytes, err := node.base.GetCodeC().Marshal(reflect.ValueOf(nodeInfo))
	if err != nil {
		log.Error("Encoding bucketInfo: %v failed: %v", *nodeInfo, err)
		return fmt.Errorf("Marshal node info body to add failed: %v", err)
	}
	payloadLen := len(payloadBytes)
	//构造头部
	var header base.Header
	header = &codec.SimpleHeader{
		Magic:  codec.MAGIC_NUM,
		Length: (int32)(payloadLen),
	}
	header.SetCmd("ConnectToOssService")
	//编码头部
	headerBytes, errB := node.base.GetCodeC().GetHeaderBytes(header)
	if errB != nil {
		log.Error("GetHeaderBytes failed%v\n", errB)
		return fmt.Errorf("Marshal node info header to add failed: %v", err)
	}
	//发送给任一一个连接的dataHandle
	session.Write(headerBytes, payloadBytes)
	node.connectFlagChan = make(chan int8, 1)
	defer func() {
		close(node.connectFlagChan)
	}()
	//此处等待，直到收到回复或者超时
	timeOutChan := time.After(5 * time.Second)
	select {
	case <-timeOutChan:
		log.Error("Connect dispatchSvr failed:time out")
		return fmt.Errorf("Connect dispatchSvr failed:time out")
	case res := <-node.connectFlagChan:
		if res == -1 {
			log.Error("Connect dispatchSvr failed")
			return fmt.Errorf("Connect dispatchSvr failed")
		}
	}

	return nil
}

func TransToByteUnit(size uint64, unit string) uint64 {
	KB := "KB"
	MB := "MB"
	GB := "GB"
	TB := "TB"

	unit = strings.ToUpper(unit)
	if strings.Compare(unit, KB) == 0 {
		return size * 1024
	} else if strings.Compare(unit, MB) == 0 {
		return size * 1024 * 1024
	} else if strings.Compare(unit, GB) == 0 {
		return size * 1024 * 1024 * 1024
	} else if strings.Compare(unit, TB) == 0 {
		return size * 1024 * 1024 * 1024 * 1024
	} else {
		return 0
	}
}
