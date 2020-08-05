package azkaban

import (
	"encoding/json"
	"fmt"
	"github.com/poemp/go-azkaban-api/inter"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type Adapter struct {
	SessionId string
}
// it's get request
//will return json string
//AzkabanConfig azkaban config
// tail request path
func (adapter Adapter) Get(config inter.AzkabanConfig, tail string) string {
	client := &http.Client{}
	request, err := http.NewRequest("GET", config.Url+tail, nil) //建立一个请求
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(0)
	}
	//Add 头协议
	request.Header.Add("Accept", "application/x-www-form-urlencoded; charset=utf-88")
	request.Header.Add("X-Requested-With", "XMLHttpRequest")
	response, err := client.Do(request) //提交
	if request == nil || err != nil {
		return "{}"
	}
	defer response.Body.Close()
	cookies := response.Cookies() //遍历cookies
	for _, cookie := range cookies {
		fmt.Println("cookie:", cookie)
	}

	body, err1 := ioutil.ReadAll(response.Body)
	if err1 != nil {
		fmt.Println("Read Response String Error ", err1.Error())
	}
	fmt.Println(string(body)) //网页源码
	return string(body)
}

// post request
//return response json string
//AzkabanConfig azkaban config
// tail request path
func (adapter Adapter) Post(config inter.AzkabanConfig, pars map[string]string, tail string) string {
	client := &http.Client{}
	resultByte, errError := json.Marshal(pars)
	if errError != nil {
		fmt.Println("Read Response String Error ", errError.Error())
		return "{}"
	}
	retest, err := http.NewRequest("POST", config.Url+tail, strings.NewReader(string(resultByte)))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(0)
	}
	retest.Header.Add("Accept", "application/x-www-form-urlencoded; charset=utf-88")
	retest.Header.Add("X-Requested-With", "XMLHttpRequest")
	resp, err := client.Do(retest)
	if resp == nil || err != nil {
		return "{}"
	}
	cookies := resp.Cookies()
	for _, cookie := range cookies {
		fmt.Println("cookie:", cookie)
	}
	defer resp.Body.Close()

	body, err1 := ioutil.ReadAll(resp.Body)
	if err1 != nil {
		fmt.Println("Read Response String Error ", err1.Error())
	}
	fmt.Println(string(body)) //网页源码
	return string(body)
}

//登录
func Login() string {
	azkabanConfig := inter.DefaultAzkabanConfig()
	par := map[string]string{
		"action":   "login",
		"username": azkabanConfig.UserName,
		"password": azkabanConfig.Password,
	}
	d := Adapter{}
	reqeust := d.Post(azkabanConfig, par, "")
	fmt.Println("Response String  ", reqeust)
	return ""
}

//创建项目
// name 项目名称 必填
//description 描述 必填
func CreateProject(name string, description string) string  {
	azkabanConfig := inter.DefaultAzkabanConfig()
}
//删除项目

//上传zip 上传依赖文件 zip包

//获取一个project的流ID

//获取一个job的流结构 依赖关系

//获取正在执行的流id

//执行

//取消执行

//Set a SLA 设置调度任务 执行的时候 或者执行成功失败等等的规则匹配 发邮件或者...

//通过cron表达式调度执行 创建调度任务

//获取一个调度器job的信息 根据project的id 和 flowId

//执行 flow

//执行信息

//获取一个执行流的日志

//查询 flow 执行情况

//重新执行一个执行流

//重新执行一个执行流
