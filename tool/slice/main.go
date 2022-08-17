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

	////查询当前用户所拥有的bucket
	//bucketSet, err := tcaplus_client.GetAllBucketOfUser(user.AccessId)
	//if err != nil {
	//	fmt.Printf("Get all buckets of user failed:%v\n", err)
	//}
	//fmt.Printf("bucket of user:%v is%v\n", user.AccessId, bucketSet)

	slice := tcaplus_uecqms.NewTb_Object_Slice()
	slice.ObjectId = "fairzhang77-bucket2-text2.txt"
	index := "Index_Obj"
	slicSet, err := tcaplus_client.GetObjectSliceByIndex(slice, index)
	if err != nil {
		fmt.Printf("Get slice of objectId:%v\n", slice.ObjectId)
	}
	for _, slice := range slicSet {
		fmt.Printf("ObjectId:%v's slice is: %v\n", slice.ObjectId, slice)
		err := tcaplus_client.DeleteObjectSlice(&slice)
		if err != nil {
			fmt.Printf("Delete slice of objectId:%v\n", slice)
		}
	}
	return
}
