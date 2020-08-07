package main

import (
	"github.com/poemp/go-azkaban-api/azkaban"
)

func main() {
	println("Hello GoLang")
	a := azkaban.AzkabanAdapter{}
	 //
	 //s , _ := a.CreateProject("GoLongProject", "GoLongProject")
	 //println(s)

	//s , _ := a.ExecutionInfo("369" ,"DATA_PUSH")
	//println(s)

	logs, _ := a.FetchExecutionJobLogs("370", "DataPush", 0, 5000)
	println(logs)
}
