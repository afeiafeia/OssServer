package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"
	"strings"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameBucket = "tb_bucket"
	ProjectName     = "TK"
)

func (t *TcaplusApi) GetBucket(req *tcaplus_uecqms.Tb_Bucket) (*tcaplus_uecqms.Tb_Bucket, error) {
	//req的主键项要有有效值，根据主键获取对应的完整记录
	rspRecord, err := t.SendAndRecv(TableNameBucket, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		log.Warn("no record")
		return nil, errors.NewWithCode(NO_RECORD, "no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_Bucket()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}

func (t *TcaplusApi) GetObjectCountOfBucket(req *tcaplus_uecqms.Tb_Bucket, index string) (int, error) {
	//req的主键项要有有效值，根据主键获取对应的完整记录
	rspRecordCount, err := t.PartSendAndRecv(TableNameBucket, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return 0, errors.Append(err, "SendAndRecv failed")
	}

	return len(rspRecordCount), nil
}

//获取一个用户的所有bucket
func (t *TcaplusApi) GetAllBucketOfUser(userId string) ([]tcaplus_uecqms.Tb_Bucket, error) {
	req := tcaplus_uecqms.NewTb_Bucket()
	req.ProjectName = ProjectName
	req.OwnerId = userId
	index := "Index_Pno"
	rspRecords, err := t.PartSendAndRecv(TableNameBucket, cmd.TcaplusApiGetByPartkeyReq, req, index)
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	records := make([]tcaplus_uecqms.Tb_Bucket, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

func (t *TcaplusApi) GetBucketsByIndex(req *tcaplus_uecqms.Tb_Bucket, index string) ([]tcaplus_uecqms.Tb_Bucket, error) {
	//log.Warn("Start PartSendAndRecv...")
	rspRecords, err := t.PartSendAndRecv(TableNameBucket, cmd.TcaplusApiGetByPartkeyReq, req, index)
	//log.Warn("PartSendAndRecv done")
	if err != nil {
		log.Error("PartSendAndRecv failed: %v", err)
		return nil, fmt.Errorf("PartSendAndRecv failed!")
	}
	if rspRecords == nil || len(rspRecords) == 0 {
		//log.Warn("no record")
		return nil, errors.NewWithCode(NO_RECORD, "no record")
	}
	records := make([]tcaplus_uecqms.Tb_Bucket, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}

//插入一条记录
func (t *TcaplusApi) InsertBucket(req *tcaplus_uecqms.Tb_Bucket) error {
	_, err := t.SendAndRecv(TableNameBucket, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		log.Warn("insert bucket failed: %v", err)
		return errors.Append(err, "insert bucket failed")
	}
	return nil
}

func (t *TcaplusApi) DeleteBucket(req *tcaplus_uecqms.Tb_Bucket) error {
	_, err := t.SendAndRecv(TableNameBucket, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed %v", err)
	}
	return nil
}

func (t *TcaplusApi) DeleteBucketByIndex(req *tcaplus_uecqms.Tb_Bucket, index string) error {
	_, err := t.PartSendAndRecv(TableNameBucket, cmd.TcaplusApiDeleteByPartkeyReq, req, index)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed %v", err)
	}
	return nil
}

//根据节点、用户、桶名称、时间戳，创建一条bucket记录
func ConstructBucketRecord(userId, bucketName string, timestamp uint64) *tcaplus_uecqms.Tb_Bucket {

	record := tcaplus_uecqms.NewTb_Bucket()
	record.ProjectName = ProjectName
	record.Name = bucketName
	record.OwnerId = userId
	record.UploadTime = timestamp
	//利用version、gameId、clientId生成testTag
	str := make([]string, 0)
	str = append(str, bucketName)
	str = append(str, userId)
	//res := strings.Join(str, "_")
	//Md5Inst := md5.New()
	//Md5Inst.Write([]byte(res))
	//record.Id = hex.EncodeToString(Md5Inst.Sum(nil))
	record.BucketId = EncodeBucketId(userId, bucketName)
	return record
}

func EncodeBucketId(userId, bucketName string) string {

	//strings.Join(str, "_")
	//Md5Inst := md5.New()
	//Md5Inst.Write([]byte(res))

	return userId + "-" + bucketName
}

func DecodeBucketId(bucketId string) (string, string) {

	//resByte,_:=hex.DecodeString(bucketId)
	//res:=string(resByte)

	res := strings.Split(bucketId, "-")
	userId := res[0]
	bucketName := res[1]
	return userId, bucketName
}
