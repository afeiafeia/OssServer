package mysql

import (
	"fmt"
	"oss/lib/log"
	"sync"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MysqlApi struct {

	// mysql地址
	Addr string
	// mysql的登陆用户名
	User string
	// 用户的密码
	Signature string
	// 访问的DB
	DbName string

	// gorm client的句柄
	client *gorm.DB

	// 锁
	mu sync.Mutex
}

func NewMysqlClient(user, passwd, addr, dbname string) (*MysqlApi, error) {

	var m *MysqlApi = new(MysqlApi)
	var err error

	m.User = user
	m.Signature = passwd
	m.Addr = addr
	m.DbName = dbname

	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, passwd, addr, dbname)

	m.client, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("mysql connect failed %v", err)
		return nil, err
	}

	return m, nil
}
