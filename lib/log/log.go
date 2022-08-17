package log

import (
	"fmt"
	"log"
	l "log"
	"os"
	"runtime"
	"strings"
	"time"
)

var (
	logConfig *Config
)

type Config struct {
	lv    Level        //日志等级
	timer *time.Timer  //定时器
	info  *LogFileInfo //日志文件的信息
}
type LogFileInfo struct {
	fullpath string   //全路径
	postfix  string   //.log，后缀
	osfile   *os.File //当前文件，使用新文件时，将原文件关闭，当前文件替换为新文件
}

func Init(lv Level, logfilepath string) {
	logConfig = &Config{
		lv: lv,
	}
	//路径下的目录可能不存在，要先创建目录
	error := os.MkdirAll(logfilepath, os.ModePerm)
	if error != nil {
		log.Fatalf("error creating filepath: %v", error)
	}

	path := logfilepath
	if !strings.HasSuffix(logfilepath, "/") {
		path += "/"
	}
	suffix := ".log"
	logConfig.info = &LogFileInfo{
		fullpath: path,
		postfix:  suffix,
		osfile:   nil,
	}

	logfile := todayToFileName()
	openLogFile(logfile)
	//更新过期时间
	expire := calExpireTime()
	//初始化定时器
	logConfig.timer = time.AfterFunc(expire, updateLogFileInfo)
}

func Log(lv Level, format string, args ...interface{}) {

	if logConfig == nil || lv <= logConfig.lv {
		msg := fmt.Sprintf(format, args...)
		var ok bool
		_, file, line, ok := runtime.Caller(2)
		if !ok {
			file = "???"
			line = 0
		}
		l.Printf("%s:%d|%s|%s", file, line, lv.String(), msg)
	}
}

func Trace(format string, args ...interface{}) {
	Log(TRACE, format, args...)
}

func Debug(format string, args ...interface{}) {
	Log(DEBUG, format, args...)
}

func Info(format string, args ...interface{}) {
	Log(INFO, format, args...)
}

func Warn(format string, args ...interface{}) {
	Log(WARN, format, args...)
}

func Error(format string, args ...interface{}) {
	Log(ERROR, format, args...)
}

func Fatal(format string, args ...interface{}) {
	Log(FATAL, format, args...)
}

func updateLogFileInfo() {
	logfile := todayToFileName()
	openLogFile(logfile)
	expire := calExpireTime()     //计算过期时间
	logConfig.timer.Reset(expire) //重置定时器，Reset的参数以纳秒为单位

}

//计算过期时间,以纳秒为单位
func calExpireTime() time.Duration {
	now := time.Now().Local()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 1, 0, time.Local) //第二天的零点过1秒
	expire := tomorrow.Sub(now)                                                         //定时器过期时间增加1秒，是为了防止由于误差，恰好在零点前一刻触发（将导致无法划分文件）
	return expire
}

func todayToFileName() string {
	cur := time.Now().Local().Format("20060102")
	logfile := logConfig.info.fullpath + cur + logConfig.info.postfix
	return logfile
}

func openLogFile(logfile string) {
	f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	//应该先通过SetOutput更改输出地，再关闭原文件,防止关闭时，其他goroutine正在向原文件写日志
	log.SetOutput(f)                      //线程安全
	l.SetFlags(l.Llongfile)               //记录输出日志的文件的路径以及行号
	l.SetFlags(l.Ldate | l.Lmicroseconds) //记录输出日志的日期以及微秒级别时间
	originFile := logConfig.info.osfile
	logConfig.info.osfile = f
	//关闭原文件
	if originFile != nil {
		originFile.Close()
	}
}
