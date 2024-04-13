package raft

import "log"
import "fmt"
import "os"

// Debugging
const Debug = false
// const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func LogInit() {
	if Debug {
		logFile, _ := os.OpenFile("log/log"+fmt.Sprintf("%d", testcnt)+".txt", os.O_RDWR|os.O_CREATE, 0766)
		log.SetOutput(logFile)
		log.SetFlags(log.Lmicroseconds)
	}
}
