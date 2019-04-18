package raft

import "fmt"
import "runtime"
import "sync"
import "time"
import "path/filepath"

// Debugging
const Debug = 1
const LockOnLog = 1

var logMu sync.Mutex;

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        if (LockOnLog > 0) {
            logMu.Lock();
        }
        _, path, lineno, ok := runtime.Caller(1);
        _, file := filepath.Split(path)

        if (ok) {
            t := time.Now();
            a = append([]interface{} { t.Format("2006-01-02 15:04:05.00"), file, lineno }, a...);
            fmt.Printf("%s [%s:%d] " + format + "\n", a...);
        }
        if (LockOnLog > 0) {
            logMu.Unlock()
        }
    }
    return
}

