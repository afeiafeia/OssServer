package dataHandle

import (
	"context"
	"oss/app"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/base"
	_ "oss/lib/base"
	"oss/lib/log"

	"github.com/rangechow/errors"
)

const (
	OSSUser = "OSS_USER"
)

//连接时的用户验证
func (dataHandler *DataHandle) VerifyUser(ctx context.Context, req *app.AccessVerify, rsp *app.AccessVerifyRsp) error {

	rsp.Ret = base.RET_ERROR
	//在tcaplus中查询
	user := tcaplus_uecqms.NewTb_User()
	user.OssUser = OSSUser
	user.AccessId = req.AccessId
	user.AccessKey = req.AccessKey
	isExist, err := dataHandler.GetTcaplusClient().IsExistUser(user)
	if err != nil {
		rsp.ErrorMsg = "there is no this user"
		log.Error("Check user info failed: %v", err)
		return nil
	}
	if !isExist {
		return nil
	}

	//当有用户连接到来的时候，将用户的所有bucket获取出来，存到内存中
	bucketRecoed := tcaplus_uecqms.NewTb_Bucket()
	bucketRecoed.ProjectName = ProjectName
	bucketRecoed.OwnerId = user.AccessId
	index := "Index_Pno"
	bucketSet, errG := dataHandler.GetTcaplusClient().GetBucketsByIndex(bucketRecoed, index)
	if errG != nil && !errors.Is(errG, tcaplus_api.NO_RECORD) {
		log.Error("Get bucket of user:%v failed:%v", user.AccessId, errG)
		return nil
	}

	dataHandler.mu.Lock()
	if _, ok := dataHandler.allUserIdBucketIdMap[user.AccessId]; !ok {
		//如果之前改用户未连接，记录其bucket
		dataHandler.allUserIdBucketIdMap[user.AccessId] = make(map[string]uint64)
		for _, bucket := range bucketSet {
			dataHandler.allUserIdBucketIdMap[user.AccessId][bucket.BucketId] = (uint64)(1)
		}
	}
	dataHandler.mu.Unlock()

	rsp.Ret = base.RET_OK
	return nil
}
