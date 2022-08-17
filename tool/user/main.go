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

	////读配置方式插入
	//for _, user := range userSet.User {
	//	record := tcaplus_uecqms.NewTb_User()
	//	record.OssUser = OSS_USER
	//	record.AccessId = user.AccessId
	//	record.AccessKey = user.AccessKey
	//	isExist, err := tcaplus_client.InsertUser(record)
	//	if err != nil {
	//		log.Error("Insert user: %v failed: %v", user, err)
	//		fmt.Printf("Insert user: %v failed: %v\n", user, err)
	//		continue
	//	}
	//	if isExist {
	//		log.Info("user: %v has exist!", user)
	//	} else {
	//		log.Info("insert user: %v success!", user)
	//	}
	//
	//}

	baseId := "fairzhang"
	baseKey := "fairzhang"
	for i := 1; i < 101; i++ {
		record := tcaplus_uecqms.NewTb_User()
		record.OssUser = OSS_USER
		record.AccessId = baseId + strconv.Itoa(i)
		record.AccessKey = baseKey + strconv.Itoa(i)
		isExist, err := tcaplus_client.InsertUser(record)
		if err != nil {
			log.Error("Insert user: %v failed: %v", record.AccessId, err)
			fmt.Printf("Insert user: %v failed: %v\n", record.AccessId, err)
			continue
		}
		if isExist {
			log.Info("user: %v has exist!", record.AccessId)
		} else {
			log.Info("insert user: %v success!", record.AccessId)
		}
	}

}
