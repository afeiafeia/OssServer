package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameBucketNeedDel = "tb_bucket_need_delete"
)

func (t *TcaplusApi) GetBucketNeedDelete() ([]tcaplus_uecqms.Tb_Bucket_Need_Delete, error) {
	//req的主键项要有有效值
	req := tcaplus_uecqms.NewTb_Bucket_Need_Delete()
	req.Flag = Flag
	index := "Index_F"
	rspRecords, err := t.PartSendAndRecv(TableNameBucketNeedDel, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	records := make([]tcaplus_uecqms.Tb_Bucket_Need_Delete, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

func (t *TcaplusApi) InsertBucketNeedDelete(req *tcaplus_uecqms.Tb_Bucket_Need_Delete) error {
	_, err := t.SendAndRecv(TableNameBucketNeedDel, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		log.Warn("insert bucket failed: %v", err)
		return errors.Append(err, "insert bucket failed")
	}
	return nil
}

func (t *TcaplusApi) DeleteBucketNeedDelete(req *tcaplus_uecqms.Tb_Bucket_Need_Delete) error {
	_, err := t.SendAndRecv(TableNameBucketNeedDel, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		log.Warn("insert bucket failed: %v", err)
		return errors.Append(err, "insert bucket failed")
	}
	return nil
}
