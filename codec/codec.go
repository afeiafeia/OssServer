package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"oss/lib/base"
	"oss/lib/log"
	"reflect"

	"github.com/rangechow/errors"
)

//从报文解析出自定义格式的消息
type SimpleCodec struct {
}

//解码出头部信息
func (s SimpleCodec) GetHeader(b *bytes.Buffer) (base.Header, error) {

	var header SimpleHeader

	if HEADER_SIZE > b.Len() {
		return nil, errors.NewWithCode(base.BUFFER_LT_HEADER, "header not enough")
	}
	len := b.Len()
	if err := binary.Read(b, binary.LittleEndian, &header); err != nil {
		log.Info("Header is:%v", header)
		log.Warn("binary.Read failed:%v with len:%v", err, len)
		return nil, errors.New("decode header failed")
	}

	if header.Magic != MAGIC_NUM {
		log.Info("Header is:%v", header)
		return nil, errors.NewWithCode(base.MAGIC_NUM_ERROR, "header magic error %v", header.Magic)
	}

	if header.Length < 0 {
		log.Info("Header is:%v", header)
		return nil, errors.NewWithCode(base.LENGTH_ERROR, "header length error %v", header.Length)
	}

	return &header, nil
}

//读取报文实体，此步还没有解码出来，需要再调用Unmarshal解码出来存入对象中
func (s SimpleCodec) GetPayload(b *bytes.Buffer, len int) ([]byte, error) {

	if b.Len() < len {
		return nil, errors.NewWithCode(base.BUFFER_LT_PAYLOAD, "payload not enough %d/%d", b.Len(), len)
	}

	payload := make([]byte, len)
	b.Read(payload)

	return payload, nil
}

//从字节序列中解析出请求报文的报文实体
func (s SimpleCodec) Unmarshal(b []byte, req reflect.Value) error {

	err := json.Unmarshal(b, req.Interface())
	if err != nil {
		log.Warn("json Unmarshal failed %v", err)
		return err
	}

	return nil
}

//编码为Json格式字节序列（针对报文实体），头部暂未采用这种编码
func (s SimpleCodec) Marshal(rsp reflect.Value) ([]byte, error) {

	b, err := json.Marshal(rsp.Interface())
	if err != nil {
		log.Warn("json Marshal failed %v", err)
		return nil, err
	}

	return b, nil
}

//编码为字节序列（针对头部）
func (s SimpleCodec) GetHeaderBytes(h base.Header) ([]byte, error) {
	b := new(bytes.Buffer)
	err := binary.Write(b, binary.LittleEndian, h)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return nil, errors.New("encode header failed")
	}
	return b.Bytes(), nil
}

//创建头部
func (s SimpleCodec) SetHeader(msgId uint32, cmd string, payloadLength int32) base.Header {

	h := &SimpleHeader{}
	h.SetMagic(MAGIC_NUM)
	h.SetMsgId(msgId)
	h.SetCmd(cmd)
	h.SetPayloadLength(payloadLength)
	return h
}
