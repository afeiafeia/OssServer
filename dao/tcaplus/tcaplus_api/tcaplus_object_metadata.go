package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameObjMeta = "tb_object_metadata"
)

func (t *TcaplusApi) GetMetadata(req *tcaplus_uecqms.Tb_Object_Metadata) (*tcaplus_uecqms.Tb_Object_Metadata, error) {

	rspRecord, err := t.SendAndRecv(TableNameObjMeta, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_Object_Metadata()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}

func (t *TcaplusApi) GetMetadataByIndex(req *tcaplus_uecqms.Tb_Object_Metadata, index string) ([]tcaplus_uecqms.Tb_Object_Metadata, error) {

	rspRecords, err := t.PartSendAndRecv(TableNameObjMeta, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	records := make([]tcaplus_uecqms.Tb_Object_Metadata, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

//获取指定bucket下的对象总数
func (t *TcaplusApi) GetObjectCount(req *tcaplus_uecqms.Tb_Object_Metadata) (uint64, error) {

	index := "Index_B"
	rspRecords, err := t.PartSendAndRecv(TableNameObjMeta, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return 0, fmt.Errorf("PartSendAndRecv failed!")
	}

	return (uint64)(len(rspRecords)), nil
}

func (t *TcaplusApi) InsertObjectMetadata(req *tcaplus_uecqms.Tb_Object_Metadata) error {
	_, err := t.SendAndRecv(TableNameObjMeta, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		log.Warn("insert objectMetadata failed: %v", err)
		return errors.Append(err, "insert bucket failed")
	}
	return nil
}

func (t *TcaplusApi) DeleteObjectMetadata(req *tcaplus_uecqms.Tb_Object_Metadata) error {
	_, err := t.SendAndRecv(TableNameObjMeta, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		log.Warn("delete bucket failed: %v", err)
		return errors.Append(err, "delete bucket failed")
	}
	return nil
}

func (t *TcaplusApi) DeleteObjectMetadataByIndex(req *tcaplus_uecqms.Tb_Object_Metadata, index string) error {
	_, err := t.PartSendAndRecv(TableNameObjMeta, cmd.TcaplusApiDeleteByPartkeyReq, req, index)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed %v", err)
	}
	return nil
}
