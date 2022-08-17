package main

import (
	//"flag"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	_ "time"

	"oss/app/dispatchSvr/dispatch"
	"oss/codec"
	"oss/lib/base"
	"oss/lib/log"

	_ "net/http/pprof"

	"github.com/spf13/viper"
)

type ForwardFunction struct {
	FunctionName string `json:"functionName"`
	Destination  string `json:"destination"`
}

type Config struct {
	DataHandle  dispatch.DataHandleSet     `json:"dataHandle"`
	Node        dispatch.NodeConfigInfoSet `json:"node"`
	ForwardRule []ForwardFunction          `json:"forwardRule"`
}
type ForwardRuleSet struct {
	ForwardRule []ForwardFunction `json:"forwardRule"`
}

func main() {

	//如果需要记录互斥锁信息，需要加上这一行
	runtime.SetMutexProfileFraction(1)
	//统计阻塞信息，需要加上这一行
	runtime.SetBlockProfileRate(1)

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
	configFileData, errR := os.ReadFile(configFilePath)
	if errR != nil {
		fmt.Printf("read file: %v failed: %v", configFilePath, err)
		return
	}
	fmt.Printf("Unmarshal file:%v\n", configFilePath)
	forwardRule := ForwardRuleSet{}
	err = json.Unmarshal(configFileData, &forwardRule)
	if err != nil {
		fmt.Printf("Unmarshal node failed: %v\n", err)
		return
	}
	fmt.Printf("Unmarshal forward rules:%v\n", forwardRule)

	nodeSet := dispatch.NodeConfigInfoSet{}
	nodeSet.Node = make([]dispatch.NodeConfigInfo, 0)
	err = json.Unmarshal(configFileData, &nodeSet)
	if err != nil {
		fmt.Printf("Unmarshal node failed: %v\n", err)
		return
	}
	fmt.Printf("nodeSet is:%v\n", nodeSet)
	dataHandleSet := dispatch.DataHandleSet{}
	dataHandleSet.DataHandle = make([]dispatch.DataHandleInfo, 0)
	err = json.Unmarshal(configFileData, &dataHandleSet)
	if err != nil {
		fmt.Printf("Unmarshal dataHandle failed: %v\n", err)
		return
	}
	fmt.Printf("dataHandle is:%v\n", dataHandleSet)
	//初始化日志
	log.Init(log.ToLevel(viper.GetString("SERVER_BASE.LOG_LEVEL")), viper.GetString("SERVER_BASE.LOG_FILE"))

	server := new(base.Base)
	var dispatchSvr dispatch.Dispatch
	var simpleCodec codec.SimpleCodec
	// 初始化服务：初始化服务框架(注册处理函数)
	err = server.Init(&dispatchSvr, simpleCodec)
	if err != nil {
		log.Fatal("server init failed")
		return
	}

	rules := make(map[string]string)
	for _, rule := range forwardRule.ForwardRule {
		funName := rule.FunctionName
		dest := rule.Destination
		rules[funName] = dest
	}
	server.InitForwardMap(rules)

	//dispatchSvr服务初始化：将连接dataHandle和node
	err = dispatchSvr.Init(server, dataHandleSet, nodeSet)
	if err != nil {
		log.Fatal("dispatch init failed")
		return
	}

	go func() {
		// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		if err := http.ListenAndServe(":6061", nil); err != nil {
			log.Fatal("http svr run failed %v", err)
		}
		os.Exit(0)
	}()

	// 启动，监听client端的连接
	server.Run(viper.GetString("SERVER_BASE.IPaddr"))
}
