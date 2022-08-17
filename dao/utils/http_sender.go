package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"oss/lib/log"
	"strconv"
	"time"

	"github.com/rangechow/errors"
	"github.com/spf13/viper"
)

var httpClient = &http.Client{}

func SendWechatInfo(sender, receiver, title, msgInfo string) error {
	reqUrl, err := GetTof4Url("/ebus/tof4_msg/api/v1/Message/SendRTXInfo")
	if err != nil {
		return err
	}
	paasToken, err := GetTof4Token()
	if err != nil {
		return err
	}

	paasId := viper.GetString("APP_LOGIC.TOF4_PAAS_ID")
	sign, timestamp, nonce := GeneSiginature(paasToken)
	values := map[string]string{
		"Sender":   sender,
		"Receiver": receiver,
		"Title":    title,
		"MsgInfo":  msgInfo,
	}
	jsonValue, _ := json.Marshal(values)

	//receiverStr := ""
	//receiverArr := viper.GetStringSlice("WECHAT_RECEIVER")
	//for i, receiver := range receiverArr {
	//receiverStr += receiver
	//if i != len(receiverArr)-1 { // bottom
	//receiverStr += ","
	//}
	//}

	req, err := http.NewRequest("POST", reqUrl, bytes.NewBuffer(jsonValue))
	if err != nil {
		log.Warn("generate http request failed: %v", err)
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-rio-paasid", paasId)
	req.Header.Add("x-rio-nonce", nonce)
	req.Header.Add("x-rio-timestamp", timestamp)
	req.Header.Add("x-rio-signature", sign)

	log.Debug("req header is %v", req.Header)

	rsp, err := httpClient.Do(req)
	if err != nil {
		log.Warn("send http request error: %v", err)
		return err
	}
	var result map[string]interface{}
	body, err := ioutil.ReadAll(rsp.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}
	log.Debug("response is %v", result)

	return nil
}

func GetTof4Url(apiAddress string) (string, error) {
	renEnv := viper.GetString("APP_LOGIC.TOF.RUN_ENV")
	fmt.Printf("APP_LOGIC.RUN_ENV:%v\n", renEnv)
	tof4ApiMap := viper.GetStringMap("APP_LOGIC.TOF.TOF4_API_ADDRESS")
	fmt.Printf("APP_LOGIC.TOF4_API_ADDRESS:%v\n", tof4ApiMap)
	reqUrlStr, ok := tof4ApiMap[renEnv].(string)
	if !ok {
		log.Warn("data type cast err")
		return "", errors.New("data type cast err")
	}
	reqUrlStr += apiAddress
	return reqUrlStr, nil
}

func GetTof4Token() (string, error) {
	renEnv := viper.GetString("APP_LOGIC.TOF.RUN_ENV")
	tof4ApiMap := viper.GetStringMap("APP_LOGIC.TOF.TOF4_PAAS_TOKEN")
	paasToken, ok := tof4ApiMap[renEnv].(string)
	if !ok {
		log.Warn("data type cast err")
		return "", errors.New("data type cast err")
	}
	return paasToken, nil
}

// 签名机制请查看TOF 签名机制文档 https://iwiki.woa.com/pages/viewpage.action?pageId=950531985
func GeneSiginature(paasToken string) (sign string, timestamp string, nonce string) {
	timestamp = fmt.Sprintf("%d", time.Now().Unix()) // 生成时间戳，注意服务器的时间与标准时间差不能大于180秒
	rand.Seed(time.Now().Unix())
	r := rand.New(rand.NewSource(time.Now().Unix()))
	nonce = strconv.Itoa(r.Intn(4096)) // 随机字符串，十分钟内不重复即可
	signStr := fmt.Sprintf("%s%s%s%s", timestamp, paasToken, nonce, timestamp)
	sign = fmt.Sprintf("%X", sha256.Sum256([]byte(signStr))) // 输出大写的结果
	return sign, timestamp, nonce
}
