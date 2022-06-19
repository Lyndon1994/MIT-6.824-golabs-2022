package raft

import (
	"log"
)

func init() {
	//logFile, err := os.OpenFile("./raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	//if err != nil {
	//	fmt.Println("open log file failed, err:", err)
	//	return
	//}
	//log.SetOutput(logFile)
	//log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}
