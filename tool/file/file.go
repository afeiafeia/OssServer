package main

import (
	"fmt"
	"os"
)

const (
	OSS_NODE = "OSS_NODE"
)

func main() {

	//路径可能不存在，先创建路径
	folder := "/home/fair/Study/Project/internship/dsperf-server/tool/file"
	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		fmt.Printf("creating folder: %v failed: %v", folder, err)
		return
	}

	filePath := "/home/fair/Study/Project/internship/dsperf-server/test/Storage/block.1"
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Open file failed:%v\n", err)
		return
	}
	testStr := "this is test!"
	_, errW := f.Write(([]byte)(testStr))
	if errW != nil {
		fmt.Printf("write failed:%v", errW)
	}

}
