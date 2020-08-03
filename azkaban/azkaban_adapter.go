package azkaban

import (
	"fmt"
	"github.com/poemp/go-azkaban-api/inter"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

// it's get request
func Get(config inter.AzkabanConfig)  {
	client := &http.Client{}
	request, err := http.NewRequest("GET", config.Url, nil) //建立一个请求
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(0)
	}
	//Add 头协议
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	request.Header.Add("Accept-Language", "ja,zh-CN;q=0.8,zh;q=0.6")
	request.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0")
	request.Header.Add("X-Requested-With","XMLHttpRequest")
	response, err := client.Do(request) //提交
	if request == nil {
		os.Exit(1)
	}
	defer response.Body.Close()
	cookies := response.Cookies() //遍历cookies
	for _, cookie := range cookies {
		fmt.Println("cookie:", cookie)
	}

	body, err1 := ioutil.ReadAll(response.Body)
	if err1 != nil {
		// handle error
	}
	fmt.Println(string(body)) //网页源码
}

// post
func Post(config inter.AzkabanConfig)  {
	client := &http.Client{}
	reqest, err := http.NewRequest("POST", config.Url, strings.NewReader("name=1&age=2"))
	if err != nil {
		// handle error
	}

	reqest.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	reqest.Header.Add("Accept-Language", "ja,zh-CN;q=0.8,zh;q=0.6")
	reqest.Header.Add("Connection", "keep-alive")
	reqest.Header.Add("Cookie", "设置cookie")
	reqest.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0")
	resp, err := client.Do(reqest)
	cookies := resp.Cookies()
	for _, cookie := range cookies {
		fmt.Println("cookie:", cookie)
	}
	defer resp.Body.Close()

	_, err1 := ioutil.ReadAll(resp.Body)
	if err1 != nil {
		// handle error
	}

	resp.Body.Read()
	fmt.Println(string())
}
//登录
func login() string {

}
//创建项目

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