package logger

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	DClient     logTopic = "CLNT"
	DCommit     logTopic = "CMIT"
	DDrop       logTopic = "DROP"
	DError      logTopic = "ERRO"
	DInfo       logTopic = "INFO"
	DLeader     logTopic = "LEAD"
	DCandi      logTopic = "CAND"
	DFollower   logTopic = "FOLO"
	DAppend     logTopic = "APPD"
	DLog        logTopic = "LOG1"
	DLog2       logTopic = "LOG2"
	DPersist    logTopic = "PERS"
	DSnap       logTopic = "SNAP"
	DTerm       logTopic = "TERM"
	DTest       logTopic = "TEST"
	DTimer      logTopic = "TIMR"
	DTrace      logTopic = "TRCE"
	DVote       logTopic = "VOTE"
	DWarn       logTopic = "WARN"
	DDisconnect logTopic = "DCON"
	DServe      logTopic = "SERV"
)

var logStart time.Time
var logVerbosity int

func init() {
	logVerbosity = getVerbosity()
	logStart = time.Now()

	// 设置日志输出到文件
	logFile, err := os.Create("log.txt")
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	log.SetOutput(logFile)

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Log(topic logTopic, format string, a ...interface{}) {
	if logVerbosity >= 1 {
		time := time.Since(logStart).Microseconds()
		// time /= 100
		prefix := fmt.Sprintf("%08d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
