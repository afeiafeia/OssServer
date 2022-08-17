package base

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
	_ "time"

	"oss/lib/log"

	"github.com/rangechow/errors"
	"github.com/rangechow/gotimer"
	uuid "github.com/satori/go.uuid"
)

var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

const REQ_TIMEOUT = 5
const MESSAGE_BUFFER_MAX_SIZE = 5 * 1024 * 1024 // IPv6 TCP MSS 1220
const BUFFER_MAX_SIZE = 16 * 1024 * 1024

type Handler struct {
	receiver reflect.Value
	method   reflect.Method
	reqProto reflect.Type
	rspProto reflect.Type
}

type ReqRspInfo struct {
	StartTime    time.Time //统计开始的时间
	ReqCount     int64
	ReqByteCount int64
	RspCount     int64
	RspByteCount int64
}

var Timer *gotimer.TimerService

type Base struct {
	isInit    bool
	isRunning bool
	mu        sync.Mutex
	//对象池:session的对象池，创建时通过对象池创建，释放时重新放入对象池
	sessionPool sync.Pool
	//Session的uid到Session的映射,记录client的连接
	sessions map[string]*Session

	taskSessionMap map[string]*Session

	sessionIndexToTaskIdMap map[string]([]string)

	//对于dispatch，与dataHandle的连接记录到此处，在base初始化的时候即进行与dataHandle的连接
	dataHandleSession map[string]*Session
	//对于dispatch，与node的连接记录到此处，在base初始化的时候即进行与node的连接
	nodeSession map[string]*Session

	listenConn net.Listener //监听
	codec      Codec
	//函数名称到函数的映射
	handlers map[string]*Handler

	//连接的管道
	connCh chan *net.Conn
	//自增id
	uid uint64

	forwardFunctionMap map[string]string

	//key是头部中的cmd,value是需要调用的处理函数的函数名
	uploadFunctionMap map[string]string

	reqRspInfo ReqRspInfo
}

func (b *Base) GetMutex() *sync.Mutex {
	return &b.mu
}
func (b *Base) GetCodeC() Codec {
	return b.codec
}
func (b *Base) GetSessions() map[string]*Session {
	return b.sessions
}

func (b *Base) GetSession(index string) *Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[index]; !ok {
		return nil
	}
	return b.sessions[index]
}

func (b *Base) AddTaskSession(taskId string, s *Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.taskSessionMap[taskId] = s
	b.sessionIndexToTaskIdMap[s.index] = append(b.sessionIndexToTaskIdMap[s.index], taskId)
	log.Debug("add task session :%v", taskId)
}
func (b *Base) GetTaskSession(taskId string) *Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.taskSessionMap[taskId]
	log.Debug("get task session :%v", taskId)
	return s
}

// svr 需要传引用，否则无法注册引用的函数
func (b *Base) Init(svr interface{}, codec Codec) error {
	if b.isInit {
		return fmt.Errorf("has init")
	}

	b.mu.Lock()
	b.uid = 0
	b.isInit = true
	b.isRunning = false
	b.sessionPool = sync.Pool{
		New: func() interface{} {
			return new(Session)
		},
	}
	b.connCh = make(chan *net.Conn, 4000)
	b.sessions = make(map[string]*Session)
	b.handlers = make(map[string]*Handler)
	b.taskSessionMap = make(map[string]*Session)
	b.forwardFunctionMap = make(map[string]string)
	b.sessionIndexToTaskIdMap = make(map[string][]string)
	b.codec = codec
	b.register(svr) //将注册svr所实现的函数，记录在b的handlers中
	b.mu.Unlock()

	return nil
}

func (b *Base) InitForwardMap(rule map[string]string) {
	b.forwardFunctionMap = rule
}

func (b *Base) Run(addr string) {

	if !b.isInit {
		log.Error("must init")
		return
	}

	if b.isRunning {
		log.Error("already running")
		return
	}
	//go b.createSession()

	b.isRunning = true
	listenConn, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("create listen socket failed")
		return
	}

	go b.createSession()
	for {
		if !b.isRunning {
			break
		}
		conn, err := listenConn.Accept()
		if err != nil {
			log.Error("Accept socket failed")
			continue
		}
		b.connCh <- &conn
	}

	log.Warn("Server Stop")
	return
}

func (b *Base) createSession() {
	for {
		conn := <-b.connCh
		//一个连接对应一个session，SERVER_SESSION表示服务器端的连接
		s := b.newSession(conn, SERVER_SESSION)
		go b.eventLoop(s, s.ProcessRequest)

	}
}

func (b *Base) AddSession(s *Session) {
	b.mu.Lock()
	b.sessions[s.index] = s
	b.mu.Unlock()
	go b.eventLoop(s, s.ProcessRequest)
}

func (b *Base) newSession(c *net.Conn, t SessionType) *Session {
	//主协程Run创建session,子协程eventLoop释放session,所以加锁保护
	b.mu.Lock()
	s := b.sessionPool.Get().(*Session)
	s.b = b
	s.c = c
	s.t = t
	//以下两步较为耗时，改为异步
	uid := uuid.NewV4()
	s.index = uid.String()
	//b.uid++
	//s.index =b. uid
	if _, ok := b.sessions[s.index]; ok {
		log.Warn("session, index %s has exist", s.GetIndex())
	}
	s.sessionKey = 0
	b.sessions[s.index] = s
	b.sessionIndexToTaskIdMap[s.index] = make([]string, 0)
	b.mu.Unlock()
	log.Debug("new session, index %s", s.GetIndex())

	return s
}

func (b *Base) releaseSession(s *Session) {
	//主协程Run创建session,子协程eventLoop释放session,所以加锁保护
	b.mu.Lock()
	log.Debug("release session, index %s", s.GetIndex())
	delete(b.sessions, s.index)
	s.recvBuffer.Truncate(0)
	if s.c != nil {
		(*s.c).Close()
	}
	for _, taskId := range b.sessionIndexToTaskIdMap[s.index] {
		delete(b.taskSessionMap, taskId)
		delete(b.sessionIndexToTaskIdMap, taskId)
	}
	sessionType := s.t
	if sessionType == CLIENT_SESSION {
		//可能是dataHandle或者dataSaveSvr断开连接了
		log.Debug("This is Client session")
		CMD := "ClearSession"
		handler, ok := b.handlers[CMD]
		if ok {
			fc := handler.method.Func
			ctx := context.Background()
			err := fc.Call([]reflect.Value{handler.receiver, reflect.ValueOf(ctx), reflect.ValueOf(s)})
			if err != nil {
				log.Error("Clear session failed:%v", err)
			}
		}
	}
	s.mu.Lock()
	s.c = nil
	//s.index = 0
	s.index = ""
	s.sessionKey = 0
	s.mu.Unlock()
	b.sessionPool.Put(s)

	b.mu.Unlock()
}

//不断读取，读取到完整报文后，进行解析、处理
//每一个客户端的连接对应一个Session，以及一个eventLoop协程用于处理该客户端的请求
//而对于该客户端的每一条请求，processCallback中又会使用一个协程进行逻辑的处理
func (b *Base) eventLoop(s *Session, processCallback ProcessCallback) {
	msgBuf := make([]byte, MESSAGE_BUFFER_MAX_SIZE)
	var header Header
	var hasHeader bool = false
	defer func() {
		//wait的作用是：主协程等待所有子协程处理完毕，再释放连接
		s.wg.Wait()
		log.Info("release session....")
		b.releaseSession(s)
		log.Info("release session done")
	}()

	for {
		if s.recvBuffer.Len() > 0 {
			(*s.c).SetReadDeadline(time.Now().Add(time.Microsecond * 1))
		}
		//读取到EOF后，err==io.EOF,可以关闭连接
		recvSize, err := (*s.c).Read(msgBuf)
		if err == io.EOF {
			log.Info("connection is closed by peer")
			return
		}
		//log.Info("read size is %v", recvSize)
		(*s.c).SetReadDeadline(time.Time{}) // 清理掉deadLine，否则后续读取的时候之前设置的deadline还会生效
		if err != nil {
			if nerr, ok := err.(net.Error); !(ok && nerr.Timeout()) {
				log.Warn("connection read error: %v", err)
				return
			}
		}
		//if recvSize==0&&s.recvBuffer.Len()==0{
		//	continue
		//}
		//每次读取最多读取MESSAGE_BUFFER_MAX_SIZE数据，之后完全写入recvBuffer,recvBuffer的缓冲区会根据写入数据动态扩展
		//因此，如果recvSize!=writeSize,一定是写入过程出错了
		writeSize, err := s.recvBuffer.Write(msgBuf[:recvSize])
		if err != nil {
			log.Warn("write message to buffer failed %v", err)
			return
		}
		if writeSize != recvSize {
			log.Warn("buffer message failed, wirtesize %d, recvsize %d", writeSize, recvSize)
			return
		}

		//获取头部
		if !hasHeader {
			header, err = b.codec.GetHeader(&s.recvBuffer)
			if err != nil {
				if errors.Is(err, BUFFER_LT_HEADER) {
					log.Debug("Read data not enough %d", recvSize)
					continue
				}

				log.Warn("decode header failed:%v", err)
				return
			}
			log.Debug("header: %s, buffer len: %d", header, s.recvBuffer.Len())
			hasHeader = true
		}

		//如果得到了完整头部，尝试获取报文实体
		if hasHeader {
			//如果缓冲区剩余内容小于头部记录的报文实体长度，说明缓冲区中是不完整的报文实体，
			//需要继续读取，直至能得到完整报文实体，然后才进行逻辑处理
			if s.recvBuffer.Len() < header.GetPayloadLength() {
				continue
			}
			payload, err := b.codec.GetPayload(&s.recvBuffer, header.GetPayloadLength())
			if err != nil {
				log.Warn("Get Payload failed %v", err)
				return
			}
			//如果是需要上报的请求，对数据进行上报
			//此处将请求的数量加一，并记录请求包含的字节数
			byteCount := header.GetPayloadLength() + HEADERSIZE
			b.mu.Lock()
			b.reqRspInfo.ReqCount++
			b.reqRspInfo.ReqByteCount += (int64)(byteCount)
			b.mu.Unlock()
			ret := processCallback(header, payload)
			if ret == CALLBACK_RETURN {
				log.Warn("processCallback failed")
				return
			}
			hasHeader = false
		}

	}
}

//为服务注册函数，svr传指针
func (b *Base) register(svr interface{}) {
	receiver := reflect.ValueOf(svr)
	methods := reflect.TypeOf(svr)
	log.Debug("%v has methods: %d", reflect.TypeOf(svr), methods.NumMethod())
	for i := 0; i < methods.NumMethod(); i++ {

		method := methods.Method(i)
		mType := method.Type
		mName := method.Name //函数名称，与头部header的cmd数据项对应

		if mType.NumIn() != 4 && mType.NumIn() != 3 {
			continue
		}

		if mType.NumOut() != 1 {
			continue
		}

		ctxType := mType.In(1)
		if !ctxType.Implements(typeOfContext) {
			continue
		}

		reqType := mType.In(2)
		if returnType := mType.Out(0); returnType != typeOfError {
			continue
		}
		if _, ok := b.handlers[mName]; ok {
			continue
		}
		var rspType reflect.Type = nil
		if mType.NumIn() == 4 {

			rspType = mType.In(3)
		}

		b.handlers[mName] = &Handler{receiver, method, reqType, rspType}
		log.Debug("Register method %v|type %v\n", mName, mType)
	}

	//检查一下是不是有转发规则函数，如果有，执行一下，初始化base中的转发规则
}

func (b *Base) IsVaildCmd(cmd string) bool {
	_, ok := b.handlers[cmd]
	return ok
}

func (b *Base) GetAllCmds() []string {
	keys := make([]string, 0, len(b.handlers))
	for k := range b.handlers {
		keys = append(keys, k)
		log.Debug("cmd:%s %d", k, len(k))
	}
	return keys
}

func (b *Base) uploadProcess(ctx context.Context, header Header, payload []byte) {
	cmd := header.GetCmd()
	//检查一下是否需要上报
	if _, ok := b.uploadFunctionMap[cmd]; !ok {
		return
	}
	methodName := b.uploadFunctionMap[cmd]
	if _, ok := b.handlers[methodName]; !ok {
		return
	}
	ctx = context.WithValue(ctx, HEADER, header)
	handler := b.handlers[methodName]
	fc := handler.method.Func
	if handler.rspProto != nil { // 如果拥有返回值，返回编码后的响应报文
		log.Debug("call rpc method %v|type %v", handler.method.Name, handler.method.Type)
		//将根据头部信息进行数据上报
		fc.Call([]reflect.Value{handler.receiver, reflect.ValueOf(ctx)})
	}
}

//根据头部信息确定要调用的函数，利用payload获取参数，调用函数执行
func (b *Base) process(ctx context.Context, header Header, payload []byte) ([]byte, error) {

	handler := b.handlers[header.GetCmd()]

	if handler == nil {
		return nil, errors.New("can not find cmd %s %s %d %v", header.GetCmd(), b.GetAllCmds(), len(header.GetCmd()), b.IsVaildCmd(header.GetCmd()))
	}

	req := reflect.New(handler.reqProto.Elem())

	cmd := header.GetCmd()
	if strings.Compare(cmd, "UploadSliceData") == 0 || (strings.Compare(cmd, "SaveObjectSliceData") == 0 && handler.rspProto != nil) {
		//特殊处理
		req = reflect.ValueOf(&payload)
	} else {
		err := b.codec.Unmarshal(payload, req)
		if err != nil {
			log.Warn("UnMashal data error: %v", err)
			return nil, errors.New("UnMashal %v failed", handler.reqProto)
		}

	}

	ctx = context.WithValue(ctx, HEADER, header)

	fc := handler.method.Func
	if handler.rspProto != nil { // 如果拥有返回值，返回编码后的响应报文
		rsp := reflect.New(handler.rspProto.Elem())
		log.Debug("call rpc method %v|type %v", handler.method.Name, handler.method.Type)
		ret := fc.Call([]reflect.Value{handler.receiver, reflect.ValueOf(ctx), req, rsp})
		errInter := ret[0].Interface()
		if errInter != nil {
			return nil, errors.New("run rpc %s err %v", header.GetCmd(), errInter.(error))
		}
		//log.Info("Response msg is %v", rsp)
		rspBytes, err := b.codec.Marshal(rsp)
		if err != nil {
			log.Warn("Marshal data error: %v", err)
			return nil, errors.New("Marshal %v failed", handler.rspProto)
		}
		return rspBytes, nil
	} else { // 如果没有返回值
		log.Debug("call rpc method %v|type %v", handler.method.Name, handler.method.Type)
		ret := fc.Call([]reflect.Value{handler.receiver, reflect.ValueOf(ctx), req})
		errInter := ret[0].Interface()
		if errInter != nil {
			return nil, errors.New("run rpc %s err %v", header.GetCmd(), errInter.(error))
		}
		return nil, nil
	}
}

func (b *Base) GetReqRspInfo() ReqRspInfo {

	b.mu.Lock()
	info := b.reqRspInfo
	//重新开始统计
	b.reqRspInfo.StartTime = time.Now()
	b.reqRspInfo.ReqCount = 0
	b.reqRspInfo.ReqByteCount = 0
	b.reqRspInfo.RspCount = 0
	b.reqRspInfo.RspByteCount = 0
	b.mu.Unlock()

	return info
}
