package main

import (
	//"flag"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	_ "time"

	"oss/app"
	"oss/app/nodeSvr/node"
	"oss/codec"
	"oss/lib/base"
	"oss/lib/log"

	"net/http"
	_ "net/http/pprof"

	"github.com/spf13/viper"
)

type DispatchInformation struct {
	DispatchServerInfo app.DispatchSvrInfo `json:"dispatchServerInfo"`
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
		log.Fatal("Fatal error config file: %s\n", err)
		return
	}
	configFileData, errR := os.ReadFile(configFilePath)
	if errR != nil {
		fmt.Printf("read file: %v failed: %v", configFilePath, err)
		return
	}

	//初始化日志
	log.Init(log.ToLevel(viper.GetString("SERVER_BASE.LOG_LEVEL")), viper.GetString("SERVER_BASE.LOG_FILE"))

	server := new(base.Base)
	var nodeSvr node.NodeSvr
	var simpleCodec codec.SimpleCodec
	// 初始化服务：初始化服务框架(注册处理函数)
	err = server.Init(&nodeSvr, simpleCodec)
	if err != nil {
		log.Fatal("server init failed")
		return
	}

	err = nodeSvr.Init(server)
	if err != nil {
		log.Fatal("nodeSvr init failed")
		return
	}

	//nodeSvr服务启动后，会检查一下配置文件中是否有dispatchSvr,如果有，尝试进行连接
	//首先从配置文件中解析出DispatchSvrInfo的信息
	dispatchSvrInfo := &DispatchInformation{}
	err = json.Unmarshal(configFileData, dispatchSvrInfo)
	if err != nil {
		//解析失败:如果没有配置相关信息，解析不会返回error,只是结果为空
		log.Warn("There is no DispatchSvr's information:%v", err)
		return
	}

	log.Info("DispatchSvr info is:%v", *dispatchSvrInfo)

	//如果解析成功，说明有dispatchSvr的连接信息，将ndoeSvr主动连接上去
	nodeSvr.ConnectToDispatchSvr(&dispatchSvrInfo.DispatchServerInfo)
	go func() {
		// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		if err := http.ListenAndServe(":6062", nil); err != nil {
			log.Fatal("http svr run failed %v", err)
		}
		os.Exit(0)
	}()
	//启动服务
	server.Run(viper.GetString("SERVER_BASE.IPaddr"))
}
