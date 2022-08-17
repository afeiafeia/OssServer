package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameUser = "tb_user"
	OssUser       = "OSS_USER"
)

func (t *TcaplusApi) GetUser(req *tcaplus_uecqms.Tb_User) (*tcaplus_uecqms.Tb_User, error) {

	rspRecord, err := t.SendAndRecv(TableNameUser, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_User()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}

func (t *TcaplusApi) GetAllUser() ([]tcaplus_uecqms.Tb_User, error) {
	req := tcaplus_uecqms.NewTb_User()
	req.OssUser = OssUser
	rspRecords, err := t.PartSendAndRecv(TableNameUser, cmd.TcaplusApiGetByPartkeyReq, req, "Index_Oss")
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	records := make([]tcaplus_uecqms.Tb_User, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

func (t *TcaplusApi) IsExistUser(req *tcaplus_uecqms.Tb_User) (bool, error) {
	rspRecords, err := t.SendAndRecv(TableNameUser, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("Check User:%v at SendAndRecv failed %v", *req, err)
		return false, fmt.Errorf("CheckBucketInfoInNode failed:%v", err)
	}
	if rspRecords == nil {
		return false, nil
	}
	return true, nil
}

func (t *TcaplusApi) InsertUser(req *tcaplus_uecqms.Tb_User) (bool, error) {
	isExist, err := t.IsExistUser(req)
	if err != nil {
		return false, fmt.Errorf("check user:%v failed: %v", *req, err)
	}
	if !isExist {
		_, err := t.SendAndRecv(TableNameUser, cmd.TcaplusApiInsertReq, req, 0)
		if err != nil {
			return false, fmt.Errorf("Insert user:%v failed: %v", *req, err)
		}
	}

	return isExist, nil
}
