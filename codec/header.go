package codec

import (
	"bytes"
	"fmt"
	"oss/lib/base"
)

//自定义通信协议的格式
const MAGIC_NUM = 28888

//头部长度
const HEADER_SIZE = 172

//自定义协议的消息头部
type SimpleHeader struct {
	Magic  uint32
	MsgId  uint32
	TaskId [base.MAX_TASKID]byte
	Length int32                  //报文实体的长度
	Cmd    [base.MAX_CMD_LEN]byte //调用的函数的名称
}

//获取头部中记录实体的长度
func (h *SimpleHeader) GetPayloadLength() int {
	return int(h.Length)
}

//获取头部中记录的函数名称，用于确定调用哪个注册的函数
func (h *SimpleHeader) GetCmd() string {
	b := h.Cmd[:]
	i := bytes.IndexByte(b, byte(0))
	if i == -1 {
		return string(b)
	}
	return string(b[:i])
}

func (h *SimpleHeader) String() string {
	return fmt.Sprintf("Magic: %d|MsgId: %d|TaskId: %s|Length: %d|Cmd: %s", h.Magic, h.MsgId, h.TaskId[:], h.Length, h.Cmd[:])
}

func (h *SimpleHeader) SetPayloadLength(l int32) {
	h.Length = l
}

func (h *SimpleHeader) GetMagic() int {
	return int(h.Magic)
}

func (h *SimpleHeader) SetMagic(m int) {
	h.Magic = uint32(m)
}

func (h *SimpleHeader) GetMsgId() uint32 {
	return h.MsgId
}

func (h *SimpleHeader) SetMsgId(m uint32) {
	h.MsgId = m
}

func (h *SimpleHeader) SetCmd(c string) {
	for i, chr := range c {
		h.Cmd[i] = byte(chr)
	}
}

func (h *SimpleHeader) SetTaskId(t string) {
	for i, chr := range t {
		h.TaskId[i] = byte(chr)
	}
}

func (h *SimpleHeader) GetTaskId() string {
	b := h.TaskId[:]
	i := bytes.IndexByte(b, byte(0))
	if i == -1 {
		return string(b)
	}
	return string(b[:i])
}
