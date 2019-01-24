package main

import (
	"fmt"
	"net/http"
)

func main() {

	// 通过http开启一个handle
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {

		// 输出一个hello world
		fmt.Fprintf(writer, "<h1>hello world! %s</h1>", request.FormValue("name"))
	})

	// 监听本地的端口8888
	http.ListenAndServe(":8888", nil)
}
