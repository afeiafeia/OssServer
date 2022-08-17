package base

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"

	"oss/lib/log"
)

type CallbackReturnType uint32

const (
	CALLBACK_CONTINUE CallbackReturnType = iota
	CALLBACK_RETURN
)

type ProcessCallback func(header Header, payload []byte) CallbackReturnType

type SessionType uint32

const (
	SERVER_SESSION SessionType = iota
	CLIENT_SESSION
)

const (
	RET_ERROR   = 400 //请求处理出错
	RET_OK      = 200 //请求处理成功
	RET_Forward = 300 //请求需要转发
)

//一条连接对应一个Session
type Session struct {
	b     *Base
	c     *net.Conn
	index string //session的uid的字符串形式
	//index      uint64
	msgCount   uint32 // 发出消息的数量
	sessionKey uint64
	t          SessionType  //类型：服务端/客户端
	recvBuffer bytes.Buffer //接收缓冲区

	mu sync.Mutex
	wg sync.WaitGroup
	//请求数量（测试使用）
	reqCount uint32

	forwardFlag bool
}

func (s *Session) DisableForward() {
	s.forwardFlag = false
}
func (s *Session) Close() {
	(*s.c).Close()
}

func (s *Session) GetIndex() string {
	return s.index
}

func (s *Session) GetMsgCount() uint32 {
	s.msgCount += 1
	return s.msgCount
}

func (s *Session) GetMutex() *sync.Mutex {
	return &(s.mu)
}
func (s *Session) GetConnection() net.Conn {
	return *s.c
}
func CreateSession(b *Base, c *net.Conn, t SessionType, index string) *Session {
	s := new(Session)
	s.b = b
	s.c = c
	s.t = t
	s.index = index
	return s
}

func (s *Session) ChangeIndex(index string) {
	s.b.mu.Lock()
	log.Debug("Session map is:%v", s.b.sessions)
	delete(s.b.sessions, s.index)
	//改变index
	s.index = index
	//改变session类型，凡是调用此函数的，一定是dataHandle或者dataSaveSvr节点，它们相对于dispatchSvr是服务端
	//因此，dispatch所持有的与它们的连接均认为是客户端
	s.t = CLIENT_SESSION
	s.b.sessions[index] = s
	log.Debug("Session map is:%v after change index", s.b.sessions)
	s.b.mu.Unlock()
}
func (s *Session) Write(headerBytes []byte, payload []byte) error {

	//更新统计的信息
	rspByteSize := len(headerBytes) + len(payload)
	s.b.mu.Lock()
	s.b.reqRspInfo.RspCount++
	s.b.reqRspInfo.RspByteCount += (int64)(rspByteSize)
	s.b.mu.Unlock()
	rspMsgBytes := make([]byte, len(headerBytes))
	copy(rspMsgBytes, headerBytes)
	for _, b := range payload {
		rspMsgBytes = append(rspMsgBytes, b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c == nil {
		log.Warn("session is released")
		return fmt.Errorf("session is released")
	}
	_, err := (*s.c).Write(rspMsgBytes)
	if err != nil {
		return fmt.Errorf("Write rsp header failed %v", err)
	}
	return nil
}

// for server
func (s *Session) ProcessRequest(header Header, payload []byte) CallbackReturnType {
	//每一条请求报文，开启一个协程进行处理
	//Add的作用：防止当前协程还没有处理完成，主协程就关闭session
	s.wg.Add(1)
	//统计请求数量
	s.mu.Lock()
	s.reqCount++
	s.mu.Unlock()
	go func() {
		defer func() {
			//s.mu.Unlock()
			defer s.wg.Done()
			if r := recover(); r != nil {
				log.Error("panic:%v", r)
				log.Error("stacktrace from panic: \n" + string(debug.Stack()))
				return
			}
		}()

		ctx := context.WithValue(context.Background(), "session", s) //将s与"session"绑定，process中可以通过session获取s
		cmd := header.GetCmd()

		//对报文的逻辑处理：分析所属的函数，然后调用函数执行，返回响应报文
		if _, ok := s.b.forwardFunctionMap[cmd]; ok {
			//投递到转发接口中
			s.ForwardMessage(header, payload)
			return
		}
		//如果报文是不必转发的，但是附带了taskId，则将taskId记录到s.b.taskSessionMap中
		taskId := header.GetTaskId()
		if strings.Compare(taskId, "") != 0 {
			//log.Debug("bind:%v to %v", taskId, &s)
			s.b.mu.Lock()
			s.b.taskSessionMap[taskId] = s
			s.b.mu.Unlock()
		}
		rspBytes, err := s.b.process(ctx, header, payload)
		if err != nil {
			log.Warn("process failed %v", err)
			return
		}
		//构造、发送响应报文
		if rspBytes != nil {
			header.SetPayloadLength(int32(len(rspBytes)))
			headerBytes, err := s.b.codec.GetHeaderBytes(header)
			if err != nil {
				log.Warn("Write rsp header failed %v", err)
				return
			}

			//log.Debug("header len: %v, body len: %v, length: %v", len(headerBytes), len(rspBytes), header.GetPayloadLength())
			err = s.Write(headerBytes, rspBytes)
			if err != nil {
				log.Warn("Write rsp header failed %v", err)
				return
			}
		}
		payload = nil
	}()

	return CALLBACK_CONTINUE
}

func (s *Session) ForwardMessage(header Header, payload []byte) error {

	log.Info("Forward message:%v", header)
	var session *Session

	taskId := header.GetTaskId()
	isRsp := false
	s.b.mu.Lock()
	log.Debug("find taskSession by taskId:%v", taskId)
	//对于请求报文，还未记录其taskId，所以找不到
	if _, ok := s.b.taskSessionMap[taskId]; !ok {
		log.Info("This is request message!")
		header.SetTaskId(s.index)
		log.Debug("Taskid is %v!", s.index)
		s.b.taskSessionMap[s.index] = s
		log.Debug("taskSessionMap is %v!", s.b.taskSessionMap)
	} else {
		//如果找到，说明之前是已经到达该请求了，此时是对请求的回复，应该根据taskId找到对应的session
		//同时，要将taskId的记录删除：已经进行了回复，后面此session就不需要了
		log.Warn("This is response message!")
		log.Debug("taskSessionMap is %v!", s.b.taskSessionMap)
		isRsp = true
		taskId = header.GetTaskId()
		log.Warn("Taskid is %v!", taskId)
		session = s.b.taskSessionMap[taskId]
		//delete(s.b.taskSessionMap, taskId)
		if session == nil {
			log.Error("can not find destination by taskId:%v!", taskId)
			//return fmt.Errorf("can not find destination!")
		}
	}
	s.b.mu.Unlock()

	if !isRsp {

		destination := s.b.forwardFunctionMap[header.GetCmd()]
		s.b.mu.Lock()
		if _, ok := s.b.sessions[destination]; ok {
			session = s.b.sessions[destination]
		}
		s.b.mu.Unlock()
		//log.Debug("destination is:%v", session.index)
		if session == nil {
			log.Error("can not find destination by header'cmd:%v!", header.GetCmd())
			//return fmt.Errorf("can not find destination!")
		}
	}
	payloadLen := len(payload)
	header.SetPayloadLength((int32)(payloadLen))
	log.Debug("Sesseion index is %v!", session.index)
	headerBytes, err := session.b.codec.GetHeaderBytes(header)
	if err != nil {
		log.Warn("Get header failed %v", err)
		return err
	}
	err = session.Write(headerBytes, payload)
	if err != nil {
		log.Error("write message failed: %v!", err)
		return err
	}
	log.Info("Forward message to dataHandle")
	return nil
}
