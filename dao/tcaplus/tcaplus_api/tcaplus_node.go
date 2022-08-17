package tcaplus_api

import (
	"fmt"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"git.woa.com/gcloud_storage_group/tcaplus-go-api/protocol/cmd"
	"github.com/rangechow/errors"
)

const (
	TableNameNode = "tb_node_info"
	Node          = "OSS_NODE"
)

func (t *TcaplusApi) GetAllNode() ([]tcaplus_uecqms.Tb_Node_Info, error) {

	req := tcaplus_uecqms.NewTb_Node_Info()
	req.Node = Node
	rspRecords, err := t.PartSendAndRecv(TableNameNode, cmd.TcaplusApiGetByPartkeyReq, req, "Index_N")
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecords == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	records := make([]tcaplus_uecqms.Tb_Node_Info, len(rspRecords))
	for index, record := range rspRecords {
		if err := record.GetData(&records[index]); err != nil {
			log.Error("GetData failed: %v at index: %v", err, index)
			return nil, fmt.Errorf("GetData failed: %v!", err)
		}
	}
	return records, nil
}
func (t *TcaplusApi) GetNode(req *tcaplus_uecqms.Tb_Node_Info) (*tcaplus_uecqms.Tb_Node_Info, error) {
	rspRecord, err := t.SendAndRecv(TableNameNode, cmd.TcaplusApiGetReq, req, 0)
	if err != nil {
		log.Warn("SendAndRecv failed %v", err)
		return nil, errors.Append(err, "SendAndRecv failed")
	}
	if rspRecord == nil {
		log.Warn("no record")
		return nil, errors.New("no record")
	}
	//通过GetData获取记录
	data := tcaplus_uecqms.NewTb_Node_Info()
	if err := rspRecord.GetData(data); err != nil {
		log.Warn("record.GetData failed %s", err)
		return nil, errors.Append(err, "record.GetData failed")
	}
	return data, nil
}
func (t *TcaplusApi) InsertNode(req *tcaplus_uecqms.Tb_Node_Info) error {
	_, err := t.SendAndRecv(TableNameNode, cmd.TcaplusApiInsertReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}

func (t *TcaplusApi) UpdateNode(req *tcaplus_uecqms.Tb_Node_Info) error {
	_, err := t.SendAndRecv(TableNameNode, cmd.TcaplusApiReplaceReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}

func (t *TcaplusApi) DeleteNode(req *tcaplus_uecqms.Tb_Node_Info) error {
	_, err := t.SendAndRecv(TableNameNode, cmd.TcaplusApiDeleteReq, req, 0)
	if err != nil {
		return fmt.Errorf("SendAndRecv failed: %v", err)
	}
	return nil
}
