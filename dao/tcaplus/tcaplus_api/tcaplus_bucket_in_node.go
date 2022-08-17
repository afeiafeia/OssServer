package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameBucketInNode = "tb_bucket_in_node"
)

func (t *TcaplusApi) GetBucketInfoInNode(req *tcaplus_uecqms.Tb_Bucket_In_Node) (*tcaplus_uecqms.Tb_Bucket_In_Node, error) {

	rspRecord, err := t.SendAndRecv(TableNameBucketInNode, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		//log.Warn("no record")
		return nil, errors.NewWithCode(NO_RECORD, "no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_Bucket_In_Node()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}

func (t *TcaplusApi) CheckBucketInfoInNode(req *tcaplus_uecqms.Tb_Bucket_In_Node) (bool, error) {

	rspRecord, err := t.SendAndRecv(TableNameBucketInNode, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return false, fmt.Errorf("CheckBucketInfoInNode failed:%v", err)
	}
	if rspRecord == nil {
		return false, nil
	}
	return true, nil
}

func (t *TcaplusApi) InsertBucketInfoInNode(req *tcaplus_uecqms.Tb_Bucket_In_Node) error {

	_, err := t.SendAndRecv(TableNameBucketInNode, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return errors.Append(err, "SendAndRecv failed")
	}
	return nil
}

func (t *TcaplusApi) DeleteBucketInfoInNode(req *tcaplus_uecqms.Tb_Bucket_In_Node) error {

	_, err := t.SendAndRecv(TableNameBucketInNode, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}

func (t *TcaplusApi) UpdateBucketInfoInNode(req *tcaplus_uecqms.Tb_Bucket_In_Node) error {

	_, err := t.SendAndRecv(TableNameBucketInNode, cmd.TcaplusApiReplaceReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return errors.Append(err, "SendAndRecv failed")
	}
	return nil
}

func (t *TcaplusApi) GetBucketInfoInNodeByIndex(req *tcaplus_uecqms.Tb_Bucket_In_Node, index string) ([]tcaplus_uecqms.Tb_Bucket_In_Node, error) {

	rspRecords, err := t.PartSendAndRecv(TableNameBucketInNode, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	if rspRecords == nil || len(rspRecords) == 0 {
		return nil, fmt.Errorf("no record")
	}
	records := make([]tcaplus_uecqms.Tb_Bucket_In_Node, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}
