package main

import (
	"github.com/poemp/go-azkaban-api/azkaban"
)

func main() {
	println("Hello GoLang")
	a := azkaban.AzkabanAdapter{}

	 s , _ := a.CreateProject("GoLongProject", "GoLongProject")
	 println(s)
}
