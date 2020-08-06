package azkaban

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/json-iterator/go"
	"github.com/poemp/go-azkaban-api/inter"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const (
	ErrorMsg   = "error"
	SuccessMsg = "success"
)

var seesion string

// this interface
type Azkaban interface {

	// 创建项目
	CreateProject(name string, description string) (string, error)

	//删除项目
	DeleteProject(projectName string) (string, error)

	//获取一个project的流ID
	FetchProjectFlows(projectName string) (string, error)

	//获取一个job的流结构 依赖关系
	FetchFlowJobs(projectName string, flowId string) (string, error)

	//执行
	FetchFlowExecutions(projectName string, flowId string, start int32, length int32) (string, error)

	//获取正在执行的流id
	FetchFlowRunningExecutions(projectName string, flowId string) (string, error)

	//执行
	ExecuteFLow(projectName string, flowId string, optionalParams map[string]string) (string, error)

	//取消执行
	CancelFlowExecution(execId string) (string, error)

	//Set a SLA 设置调度任务 执行的时候 或者执行成功失败等等的规则匹配 发邮件或者...
	//Schedule a period-based Flow
	SchedulePeriodBasedFlow(projectName string, flowName string, scheduleDate string, scheduleTime string, period string) (string, error)

	//通过cron表达式调度执行 创建调度任务
	ScheduleCronBasedFlow(projectName string, flowName string, cronExpression string) (string, error)

	// 获取一个调度器job的信息 根据project的id 和 flowId
	// Flexible scheduling using Cron
	// 通过cron表达式调度执行 创建调度任务
	ScheduleFlow(projectName string, flowName string, cronExpression string) (string, error)

	//执行 flow
	StartFlow(projectName string, flowName string, optionalParams map[string]string) (string, error)

	//执行信息
	ExecutionInfo(execId string) (string, error)

	//获取一个执行流的日志
	FetchExecutionJobLogs(execId string, jobId string, offset int32, length int32) (string, error)

	// 查询 flow 执行情况
	FetchFlowExecution(execId string) (string, error)

	//重新执行一个执行流
	FetchPauseFlow(execId string) (string, error)

	//重新执行一个执行流
	FetchResumeFlow(execId string) (string, error)
}

type adapter struct {
	SessionId string
}

// it's get request
//will return json string
//AzkabanConfig azkaban config
// tail request path
func (adapter) Get(config inter.AzkabanConfig, tail string) (string, error) {
	client := &http.Client{}
	request, err := http.NewRequest("GET", config.Url+tail, nil) //建立一个请求
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(0)
	}
	//Add 头协议
	request.Header.Add("Accept", "application/x-www-form-urlencoded; charset=utf-8")
	request.Header.Add("X-Requested-With", "XMLHttpRequest")
	response, err := client.Do(request) //提交
	if request == nil || err != nil {
		return "{}", errors.New(" this http is error, please check .")
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
	return string(body), nil
}

// post request
//return response json string
//AzkabanConfig azkaban config
// tail request path
func (adapter) Post(config inter.AzkabanConfig, pars map[string]string, tail string) (string, error) {
	client := &http.Client{}
	resultByte, errError := json.Marshal(pars)
	if errError != nil {
		fmt.Println("Read Response String Error ", errError.Error())
		return "{}", errors.New("Read Response String Error ")
	}
	retest, err := http.NewRequest("POST", config.Url+tail, strings.NewReader(string(resultByte)))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(0)
	}

	retest.Header.Add("X-Requested-With", "XMLHttpRequest")
	retest.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(retest)
	if err != nil {
		return "{}", errors.New(" this http is error, please check . " + err.Error())
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
	return string(body), nil
}

//模拟提交form表单
func (adapter) PostFrom(config inter.AzkabanConfig, pars map[string]string, tail string) (string, error) {
	client := &http.Client{}

	//post要提交的数据
	dataUrlVal := url.Values{}
	for key, val := range pars {
		dataUrlVal.Add(key, val)
	}

	retest, err := http.NewRequest("POST", config.Url + tail, strings.NewReader(dataUrlVal.Encode()))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		return "", err
	}

	retest.Header.Add("Accept", "application/json, text/javascript, */*; q=0.01")
	retest.Header.Add("X-Requested-With", "XMLHttpRequest")
	retest.Header.Add("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36")
	retest.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(retest)
	if err != nil {
		return "{}", errors.New(" this http is error, please check . " + err.Error())
	}
	cookies := resp.Cookies()
	for _, cookie := range cookies {
		fmt.Println("Cookie:", cookie)
	}
	defer resp.Body.Close()

	body, err1 := ioutil.ReadAll(resp.Body)
	if err1 != nil {
		fmt.Println("Read Response String Error ", err1.Error())
	}
	return string(body), nil
}

//登录 126
func (adapter) Login() (string, error) {
	azkabanConfig := inter.DefaultAzkabanConfig()
	pars := map[string]string{
		"action":   "login",
		"username": azkabanConfig.UserName,
		"password": azkabanConfig.Password,
	}
	d := adapter{}
	body, err := d.PostFrom(azkabanConfig, pars, "")
	if err != nil {
		return "", err
	}
	jsonData := jsoniter.Get([]byte(body))
	message := jsonData.Get("message").ToString()
	if "" != message {
		return "", errors.New(message)
	}
	seesion = jsonData.Get("session.id").ToString()
	return seesion, nil
}

// azkaban adapter
type AzkabanAdapter struct {
}

//创建项目
// name 项目名称 必填
//description 描述 必填
func (a AzkabanAdapter) CreateProject(name string, description string) (string, error) {
	azkabanConfig := inter.DefaultAzkabanConfig()

	d := adapter{}
	if seesion == "" {
		_, err := d.Login()
		if err != nil {
			return ErrorMsg, err
		}
	}

	parameters := map[string]string{
		"session.id":  seesion,
		"action":      "create",
		"name":        name,
		"description": description,
	}

	request, _ := d.PostFrom(azkabanConfig, parameters, "/manager")
	fmt.Printf("Azkaban Create Project Request:" + request)
	jsonData := jsoniter.Get([]byte(request))
	status := jsonData.Get("status").ToString()
	if "success" == status {
		return "success", nil
	}
	errorMessage := jsonData.Get("message").ToString()
	return errorMessage, errors.New(errorMessage)
}

//删除项目
func (a AzkabanAdapter) DeleteProject(projectName string) (string, error) {
	return "{}", nil
}

//获取一个project的流ID
func (a AzkabanAdapter) FetchProjectFlows(projectName string) (string, error) {
	return "{}", nil
}

//获取一个job的流结构 依赖关系
func (a AzkabanAdapter) FetchFlowJobs(projectName string, flowId string) (string, error) {
	return "{}", nil
}

//执行
func (a AzkabanAdapter) FetchFlowExecutions(projectName string, flowId string, start int32, length int32) (string, error) {
	return "{}", nil
}

//获取正在执行的流id
func (a AzkabanAdapter) FetchFlowRunningExecutions(projectName string, flowId string) (string, error) {
	return "{}", nil
}

//执行
func (a AzkabanAdapter) ExecuteFLow(projectName string, flowId string, optionalParams map[string]string) (string, error) {
	return "{}", nil
}

//取消执行
func (a AzkabanAdapter) CancelFlowExecution(execId string) (string, error) {
	return "{}", nil
}

//Set a SLA 设置调度任务 执行的时候 或者执行成功失败等等的规则匹配 发邮件或者...
//Schedule a period-based Flow
func (a AzkabanAdapter) SchedulePeriodBasedFlow(projectName string, flowName string, scheduleDate string, scheduleTime string, period string) (string, error) {
	return "{}", nil
}

//通过cron表达式调度执行 创建调度任务
func (a AzkabanAdapter) ScheduleCronBasedFlow(projectName string, flowName string, cronExpression string) (string, error) {
	return "{}", nil
}

// 获取一个调度器job的信息 根据project的id 和 flowId
// Flexible scheduling using Cron
// 通过cron表达式调度执行 创建调度任务
func (a AzkabanAdapter) ScheduleFlow(projectName string, flowName string, cronExpression string) (string, error) {
	return "{}", nil
}

//执行 flow
func (a AzkabanAdapter) StartFlow(projectName string, flowName string, optionalParams map[string]string) (string, error) {
	return "{}", nil
}

//执行信息
func (a AzkabanAdapter) ExecutionInfo(execId string) (string, error) {
	return "{}", nil
}

//获取一个执行流的日志
func (a AzkabanAdapter) FetchExecutionJobLogs(execId string, jobId string, offset int32, length int32) (string, error) {
	return "{}", nil
}

// 查询 flow 执行情况
func (a AzkabanAdapter) FetchFlowExecution(execId string) (string, error) {
	return "{}", nil
}

//重新执行一个执行流
func (a AzkabanAdapter) FetchPauseFlow(execId string) (string, error) {
	return "{}", nil
}

//重新执行一个执行流
func (a AzkabanAdapter) FetchResumeFlow(execId string) (string, error) {
	return "{}", nil
}
