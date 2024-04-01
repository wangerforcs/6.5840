package main

import (
	//	"bytes"
	"fmt"
	"time"
)

func main(){
	chan1 := make(chan int, 1)
	chan2 := make(chan int, 1)

	go func() {
		for{
		time.Sleep(time.Duration(100) * time.Millisecond)
		chan1 <- 1
		}
	}()

	go func() {
		for{
		time.Sleep(time.Duration(100) * time.Millisecond)
		chan2 <- 2
		}
	}()
	for{
		// fmt.Printf("%v\n", time.Now())
		select {
		case <-chan1:
			fmt.Println("chan1")
		case <-chan2:
			fmt.Println("chan2")
		case <-time.After(time.Duration(110) * time.Millisecond): // 只有在其他通道在给定时间内阻塞时，才会执行
			fmt.Println("timeout")
		}
	}
}