package utils

import (
	"os"
	"oss/lib/log"
)

//存储管理所打开文件的句柄，避免因频繁打开关闭文件带来性能瓶颈
const CacheCapacity int = 64

//容器
type OpenFileCache struct {
	size       int
	capacity   int
	cache      map[string]*OpenFile
	head, tail *OpenFile
}

//节点
type OpenFile struct {
	file       *os.File
	fileName   string
	prev, next *OpenFile
}

func initOpenFile(fileName string, file *os.File) *OpenFile {
	return &OpenFile{
		file:     file,
		fileName: fileName,
	}
}

func NewOpenFileCache(capacity int) *OpenFileCache {
	l := &OpenFileCache{
		cache:    map[string]*OpenFile{},
		head:     initOpenFile("", nil),
		tail:     initOpenFile("", nil),
		capacity: capacity,
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

func (this *OpenFileCache) Get(fileName string) *os.File {
	if _, ok := this.cache[fileName]; !ok {
		return nil
	}
	openFile := this.cache[fileName]
	this.moveToHead(openFile)
	return openFile.file
}

func (this *OpenFileCache) Put(fileName string, file *os.File) {
	if _, ok := this.cache[fileName]; !ok {
		openFile := initOpenFile(fileName, file)
		this.cache[fileName] = openFile
		this.addToHead(openFile)
		this.size++
		if this.size > this.capacity {
			removed := this.removeTail()
			err := removed.file.Close()
			if err != nil {
				log.Warn("close file error: %v", err)
			}
			delete(this.cache, removed.fileName)
			this.size--
		}
	} else {
		openFile := this.cache[fileName]
		openFile.file = file
		this.moveToHead(openFile)
	}
}

func (this *OpenFileCache) addToHead(openFile *OpenFile) {
	openFile.prev = this.head
	openFile.next = this.head.next
	this.head.next.prev = openFile
	this.head.next = openFile
}

func (this *OpenFileCache) removeNode(openFile *OpenFile) {
	openFile.prev.next = openFile.next
	openFile.next.prev = openFile.prev
}

func (this *OpenFileCache) moveToHead(openFile *OpenFile) {
	this.removeNode(openFile)
	this.addToHead(openFile)
}

func (this *OpenFileCache) removeTail() *OpenFile {
	openFile := this.tail.prev
	this.removeNode(openFile)
	return openFile
}
