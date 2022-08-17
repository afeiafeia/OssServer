package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"
	"strconv"
	"strings"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameObjSlice = "tb_slice"
)

func (t *TcaplusApi) GetObjectSlice(req *tcaplus_uecqms.Tb_Slice) (*tcaplus_uecqms.Tb_Slice, error) {

	rspRecord, err := t.SendAndRecv(TableNameObjSlice, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_Slice()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}

func (t *TcaplusApi) GetSliceSetOfObject(req *tcaplus_uecqms.Tb_Slice) ([]tcaplus_uecqms.Tb_Slice, error) {

	index := "Index_Obj"
	rspRecords, err := t.PartSendAndRecv(TableNameObjSlice, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecords == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	//通过GetData获取记录
	sliceSet := make([]tcaplus_uecqms.Tb_Slice, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&sliceSet[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}

	return sliceSet, nil
}

func (t *TcaplusApi) GetObjectSliceByIndex(req *tcaplus_uecqms.Tb_Slice, index string) ([]tcaplus_uecqms.Tb_Slice, error) {

	rspRecords, err := t.PartSendAndRecv(TableNameObjSlice, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	records := make([]tcaplus_uecqms.Tb_Slice, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

func (t *TcaplusApi) DeleteObjectSlice(req *tcaplus_uecqms.Tb_Slice) error {
	_, err := t.SendAndRecv(TableNameObjSlice, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}

func (t *TcaplusApi) InsertObjectSlice(req *tcaplus_uecqms.Tb_Slice) error {
	_, err := t.SendAndRecv(TableNameObjSlice, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}

func (t *TcaplusApi) DeleteObjectSliceByIndex(req *tcaplus_uecqms.Tb_Slice, index string) error {
	_, err := t.PartSendAndRecv(TableNameObjSlice, cmd.TcaplusApiDeleteByPartkeyReq, req, index)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed %v", err)
	}
	return nil
}

func EncodeObjectId(userId, bucketName, objectName string) string {
	return userId + "-" + bucketName + "-" + objectName
}

func DecodeObjectId(objectId string) (string, string, string) {
	res := strings.Split(objectId, "-")
	userId := res[0]
	bucketName := res[1]
	ObjectName := res[2]
	return userId, bucketName, ObjectName
}

func EncodeLocationId(offset, num, nodeId, version uint64, blockPath, objId string) string {
	offsetStr := strconv.Itoa((int)(offset))
	numStr := strconv.Itoa((int)(num))
	nodeIdStr := strconv.Itoa((int)(nodeId))
	versionStr := strconv.Itoa((int)(version))
	return blockPath + "+" + objId + "+" + versionStr + "+" + offsetStr + "+" + numStr + "+" + nodeIdStr
}

func DecodeLocationId(locationId string) (string, string, uint64, uint64, uint64, uint64) {
	res := strings.Split(locationId, "+")
	blockPath := res[0]
	objId := res[1]
	version, errV := strconv.Atoi(res[2])
	if errV != nil {
		log.Error("strconv.Atoi(%v) failed:%v", res[2], errV)
	}

	offset, errO := strconv.Atoi(res[3])
	if errO != nil {
		log.Error("strconv.Atoi(%v) failed:%v", res[3], errO)
	}
	sliceNum, errS := strconv.Atoi(res[4])
	if errS != nil {
		log.Error("strconv.Atoi(%v) failed:%v", res[4], errS)
	}
	nodeId, errN := strconv.Atoi(res[5])
	if errN != nil {
		log.Error("strconv.Atoi(%v) failed:%v", res[5], errN)
	}
	return blockPath, objId, (uint64)(version), (uint64)(offset), (uint64)(sliceNum), (uint64)(nodeId)
}
