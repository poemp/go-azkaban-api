package azkaban

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/poemp/go-azkaban-api/inter"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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
	request.Header.Add("Accept", "application/x-www-form-urlencoded; charset=utf-88")
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
	if seesion != "" {
		pars["session.id"] = seesion
	} else {
		d := adapter{}
		_, _ = d.Login()
		pars["session.id"] = seesion
	}
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

	retest.Header.Add("Accept", "application/x-www-form-urlencoded; charset=utf-88")
	retest.Header.Add("X-Requested-With", "XMLHttpRequest")
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
	fmt.Println(string(body)) //网页源码
	return string(body), nil
}

//登录 126
func (adapter) Login() (string, error) {
	azkabanConfig := inter.DefaultAzkabanConfig()
	par := map[string]string{
		"action":   "login",
		"username": azkabanConfig.UserName,
		"password": azkabanConfig.Password,
	}
	d := adapter{}
	reqeust, _ := d.Post(azkabanConfig, par, "")
	fmt.Println("Response String  ", reqeust)
	seesion = ""
	return "", nil
}

// azkaban adapter
type AzkabanAdapter struct {
}

//创建项目
// name 项目名称 必填
//description 描述 必填
func (a AzkabanAdapter) CreateProject(name string, description string) (string, error) {
	azkabanConfig := inter.DefaultAzkabanConfig()
	return "{}", nil
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
