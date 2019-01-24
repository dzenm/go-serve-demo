package main

import (
	"fmt"
)

// chan类型的使用，以及goroutine的使用
func main() {
	ch := make(chan string)

	// go starts a goroutine 开启goroutine将结果传入一个chan
	for i := 0; i < 10000; i++ {
		go printlnHello(i, ch)
	}

	// 将chan里的结果输出
	for {
		msg := <-ch
		fmt.Println(msg)
	}
}

// 循环输出并将结果传入chan
func printlnHello(position int, ch chan string) {
	for {
		ch <- fmt.Sprintf("hello world from goroutine %d\n", position)
	}
}
