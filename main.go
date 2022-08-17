package main

import (
	//"flag"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	_ "time"

	"oss/app/dataHandle"
	"oss/codec"
	"oss/lib/base"
	"oss/lib/log"

	_ "net/http/pprof"

	"github.com/spf13/viper"
)

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

	//初始化日志
	log.Init(log.ToLevel(viper.GetString("SERVER_BASE.LOG_LEVEL")), viper.GetString("SERVER_BASE.LOG_FILE"))

	server := new(base.Base)

	var dbSvr dataHandle.DataHandle
	var simpleCodec codec.SimpleCodec
	// 初始化服务：初始化服务框架(注册处理函数)
	err = server.Init(&dbSvr, simpleCodec)
	if err != nil {
		log.Fatal("server init failed")
		return
	}

	// 初始化服务：初始化服务逻辑:连接数据库
	if err = dbSvr.Init(server); err != nil {
		log.Fatal("svr init failed %v", err)
		return
	}

	go func() {
		// 启动一个 http server，注意 pprof 相关的 handler 已经自动注册过了
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatal("http svr run failed %v", err)
		}
		os.Exit(0)
	}()

	// 启动
	server.Run(viper.GetString("SERVER_BASE.IPaddr"))
}
