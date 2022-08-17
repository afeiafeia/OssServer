package main

import (
	"flag"
	"fmt"
	"oss/dao/tcaplus/tcaplus_api"
	"oss/dao/tcaplus/tcaplus_uecqms"
	"oss/lib/log"

	"github.com/spf13/viper"
)

const (
	OSS_NODE = "OSS_NODE"
)

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
	//删除node信息
	nodeRecord := tcaplus_uecqms.NewTb_Node_Info()
	nodeRecord.Node = OSS_NODE
	nodeSet, err := tcaplus_client.GetAllNode()
	if err != nil {
		fmt.Printf("Get all node failed!\n")
	}

	//删除node
	//for _, node := range nodeSet {
	//	err = tcaplus_client.DeleteNode(&node)
	//	if err != nil {
	//		fmt.Printf("delete node:%v failed", node)
	//	}
	//}

	//查看bucketInNode信息
	for _, node := range nodeSet {
		fmt.Printf("Node info:%v\n", node)
		nodeId := node.Id
		req := tcaplus_uecqms.NewTb_Bucket_In_Node()
		req.NodeId = nodeId
		index := "Index_N"
		bucketInNodeSet, err := tcaplus_client.GetBucketInfoInNodeByIndex(req, index)
		if err != nil {
			fmt.Printf("Get bucket in node failed!\n")
		}
		fmt.Printf("All bucket in node:%v info:%v\n", nodeId, bucketInNodeSet)
		//continue
		for _, curbucketInNode := range bucketInNodeSet {
			err = tcaplus_client.DeleteBucketInfoInNode(&curbucketInNode)
			if err != nil {
				fmt.Printf("delete node:%v failed", node)
			}
		}
	}
	return
}
