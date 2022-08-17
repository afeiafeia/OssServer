package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"oss/app"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"
	"strconv"

	"github.com/spf13/viper"
)

const (
	OSS_USER = "OSS_USER"
)

type UserSet struct {
	User []app.AccessVerify `json:"user"`
}

func main() {

	var configFilePath string
	flag.StringVar(&configFilePath, "config", "", "参数配置文件的路径")
	flag.Parse()

	if flag.NFlag() != 1 {
		fmt.Println("需要输入以下参数并配置相关文件信息")
		flag.PrintDefaults()
		return
	}
	viper.SetConfigFile(configFilePath)
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Fatal("Fatal error config file: %s \n", err)
		return
	}

	log.Init(log.ToLevel(viper.GetString("LOG_BASE.LOG_LEVEL")), viper.GetString("LOG_BASE.LOG_FILE"))
	configFileData, errR := os.ReadFile(configFilePath)
	if errR != nil {
		fmt.Printf("read file: %v failed: %v\n", configFilePath, errR)
		return
	}

	userSet := UserSet{}
	userSet.User = make([]app.AccessVerify, 0)
	errU := json.Unmarshal(configFileData, &userSet)
	if errU != nil {
		log.Error("Unmarshal user data failed!\n")
		fmt.Printf("Unmarshal user data failed!\n")
		return
	}

	tcaplus_client, errN := tcaplus_api.NewTcaplusClent(
		uint64(viper.GetInt("TCAPLUS.TC_AppId")),
		uint32(viper.GetInt("TCAPLUS.TC_ZoneId")),
		viper.GetString("TCAPLUS.TC_DirUrl"),
		viper.GetString("TCAPLUS.TC_Signature"),
		viper.GetString("TCAPLUS.LOG_CONFIG"))
	if errN != nil {
		log.Error("connect to tcaplus failed %v", errN)
		return
	}

	//
	baseId := "fairzhang"
	baseKey := "fairzhang"
	//var wg sync.WaitGroup
	//for _, user := range userSet.User {
	for i := 1; i < 101; i++ {
		//wg.Add(1)
		//go func(userId int) {
		//	go DeleteBucketRecords(userId)
		//	wg.Done()
		//}(i)
		//fmt.Printf("i:%v\n", i)
		//continue

		record := tcaplus_uecqms.NewTb_User()
		record.OssUser = OSS_USER
		//record.AccessId = user.AccessId
		record.AccessId = baseId + strconv.Itoa(i)
		//record.AccessKey = user.AccessKey
		record.AccessKey = baseKey + strconv.Itoa(i)

		//查询当前用户所拥有的bucket
		bucketSet, err := tcaplus_client.GetAllBucketOfUser(record.AccessId)
		if err != nil {
			fmt.Printf("Get all buckets of user failed:%v\n", err)
		}
		fmt.Printf("bucket of user:%v is%v\n", record.AccessId, bucketSet)

		//bucketId := "fairzhang1_example_bucket002"
		//根据bucketId查询对象
		//objMeta := tcaplus_uecqms.NewTb_Object_Metadata()
		//objMeta.BucketId = bucketId
		//index := "Index_B"
		//objSet, err := tcaplus_client.GetMetadataByIndex(objMeta, index)
		//if err != nil {
		//	fmt.Printf("Get metadata of bucket:%v failed:%v\n", bucketId, err)
		//}
		//fmt.Printf("metadata of bucket:%v is: %v\n", bucketId, objSet)
		//slice := tcaplus_uecqms.NewTb_Object_Slice()
		//slice.ObjectId = "fairzhang1-example_bucket003-08.png"
		//index = "Index_Obj"
		//slicSet, err := tcaplus_client.GetObjectSliceByIndex(slice, index)
		//if err != nil {
		//	fmt.Printf("Get slice of objectId:%v\n", slice.ObjectId)
		//}
		//fmt.Printf("ObjectId:%v's slice is: %v\n", slice.ObjectId, slicSet)
		//return
		for _, bucket := range bucketSet {
			//if strings.Compare(bucket.Name, "bucket1") == 0 {
			//	continue
			//}
			bucketId := bucket.BucketId
			//根据bucketId查询对象
			objMeta := tcaplus_uecqms.NewTb_Object_Metadata()
			objMeta.BucketId = bucketId
			index := "Index_B"
			objSet, err := tcaplus_client.GetMetadataByIndex(objMeta, index)
			if err != nil {
				fmt.Printf("Get metadata of bucket:%v failed:%v\n", bucketId, err)
			}
			//fmt.Printf("metadata of bucket:%v is: %v\n", bucketId, objSet)
			//continue
			//return
			//通过objId获取分片
			for _, obj := range objSet {
				objId := obj.ObjectId
				//slice := tcaplus_uecqms.NewTb_Object_Slice()
				//userId := record.AccessId
				//bucketName := bucket.Name
				//objectName := obj.ObjectName
				//slice.ObjectId = userId + "-" + bucketName + "-" + objectName
				//index := "Index_Obj"
				//slicSet, err := tcaplus_client.GetObjectSliceByIndex(slice, index)
				//if err != nil {
				//	fmt.Printf("Get slice of objectId:%v\n", objId)
				//}
				////fmt.Printf("ObjectId:%v's slice is: %v\n", objId, slicSet)
				////return
				//////删除分片的记录
				//for _, slice := range slicSet {
				//	err := tcaplus_client.DeleteObjectSlice(&slice)
				//	if err != nil {
				//		fmt.Printf("Delete slice of objectId:%v\n", objId)
				//		continue
				//	}
				//}
				//return
				//在删除完分片后，将object删除
				err = tcaplus_client.DeleteObjectMetadata(&obj)
				if err != nil {
					fmt.Printf("Delete slice of objectId:%v\n", objId)
					continue
				}
				//return
			}
			//return
			//再将bucket删除
			err = tcaplus_client.DeleteBucket(&bucket)
			if err != nil {
				fmt.Printf("Delete Bucket failed:%v\n", err)
				continue
			}

		}
		//
	}
	//time.Sleep(20 * time.Second)
	//fmt.Printf("Done\n")
	//wg.Wait()

	//删除bucket

}
