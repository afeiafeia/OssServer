//对磁盘文件进行读写

package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io/ioutil"
	"os"
	"oss/lib/log"
	"reflect"
	"strings"

	"github.com/rangechow/errors"
)

// 数据落地到磁盘文件，data是指针
func SaveDataToFile(filePath string, data interface{}) error {

	// 对数据进行编码，直接保存二进制
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	enc.Encode(data)

	rawdata := buffer.Bytes()
	err := ioutil.WriteFile(filePath, rawdata, 0644)
	if err != nil {
		log.Error("write data to %v failed %v", filePath, err)
		return errors.New("write data to %v failed %v", filePath, err)
	}

	return nil
}

// 从磁盘读取数据,data是指针
func ReadDataFromFile(filePath string, data interface{}) error {
	rawdata, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Error("read file %v failed %v", filePath, err)
		return errors.New("read file %v failed %v", filePath, err)
	}

	buffer := bytes.NewBuffer(rawdata)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(data)
	if err != nil {
		log.Warn("Decode objectCreateInfo failed %v", err)
		return errors.New("Decode objectCreateInfo failed %v", err)
	}

	return nil
}

func GenerateFileName(nameSet []string) string {
	if len(nameSet) == 0 {
		return ""
	}
	filePath := strings.Join(nameSet, "/")
	filePath += ".data"
	return filePath
}

// 通过缓存来保存数据,该方法为append写入
func EncodeDataToFile(filePath string, data interface{}, openFileList *OpenFileCache) error {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	enc.Encode(data)

	// 插入记录的详细信息
	rawdata := buffer.Bytes()
	saveData := make([]byte, 0, 4+len(rawdata)) // 预先分配空间

	openFile := openFileList.Get(filePath)
	if openFile == nil {
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Error("open file %v error %v", filePath, err)
			return errors.New("open file %v error %v", filePath, err)
		}
		openFileList.Put(filePath, file)
		openFile = file
	}
	//TODO 比较预先分配空间性能好还是write两次性能好
	dataLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataLength, uint32(len(rawdata)))
	saveData = append(append(saveData, dataLength...), rawdata...)

	_, err := openFile.Write(saveData)
	if err != nil {
		log.Error("write data to %v failed %v", filePath, err)
		return errors.New("write data to %v failed %v", filePath, err)
	}

	err = openFile.Sync()
	if err != nil {
		log.Error("sync data failed %v", err)
		return errors.New("sync data failed %v", err)
	}

	return nil
}

// 从磁盘读取批量数据
func ReadBatchDataFromFile(data_file_path string, dataType reflect.Type) ([]interface{}, error) {
	rawdata, err := ioutil.ReadFile(data_file_path)
	if err != nil {
		log.Error("read file %v failed %v", data_file_path, err)
		return nil, errors.New("read file %v failed %v", data_file_path, err)
	}

	var result []interface{}

	rawDataBuffer := bytes.NewBuffer(rawdata)
	for rawDataBuffer.Len() != 0 {
		dataInstanceVal := reflect.New(dataType)
		dataIns := dataInstanceVal.Interface()
		err = ParseFileData(rawDataBuffer, dataIns)
		if err != nil {
			return nil, err
		}
		result = append(result, dataIns)
	}

	return result, nil
}
func ParseFileData(b *bytes.Buffer, data interface{}) error {
	if b.Len() < 4 {
		log.Error("the data length is not correct")
		return errors.New("the data length is not correct")
	}

	var dataLength uint32
	if err := binary.Read(b, binary.LittleEndian, &dataLength); err != nil {
		log.Error("binary.Read failed:", err)
		return errors.New("decode file header failed")
	}
	if uint32(b.Len()) < dataLength {
		return errors.New("file payload not enough")
	}
	payload := make([]byte, dataLength)
	b.Read(payload)
	readBuffer := bytes.NewBuffer(payload)
	dec := gob.NewDecoder(readBuffer)
	err := dec.Decode(data)
	if err != nil {
		return errors.New("unmarshal data failed:%v", err)
	}
	return nil
}
