package main

import (
	//	"bytes"
	"fmt"
	"sync"
)

func main(){
	mu := sync.Mutex{}
	for i:=0;i<4;i++{
		mu.Lock()
		defer mu.Unlock()
		defer fmt.Printf("defer %d\n", i)
	}
	defer fmt.Println("defer 4")
}