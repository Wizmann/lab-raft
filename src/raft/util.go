package raft

import "fmt"
import "runtime"
import "sync"
import "time"
import "path/filepath"
import "math/rand"

// Debugging
const Debug = 0

var logMu sync.Mutex;

func DPrintf(format string, a ...interface{}) {
    if Debug > 0 {
        _, path, lineno, ok := runtime.Caller(1);
        _, file := filepath.Split(path)

        if (ok) {
            t := time.Now();
            a = append([]interface{} { t.Format("2006-01-02 15:04:05.00"), file, lineno }, a...);
            fmt.Printf("%s [%s:%d] " + format + "\n", a...);
        }
    }
}

// a pseudo uuid generator to create uniq ID for a log entry
// not required in Raft algorithm, only used for test
func CreateLogId() (uuid string) {
    b := make([]byte, 16)
    _, err := rand.Read(b)
    if err != nil {
        fmt.Println("Error: ", err)
        return
    }

    uuid = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

    return
}

func Assert(flag bool, format string, a ...interface{}) {
    if (!flag) {
        _, path, lineno, ok := runtime.Caller(1);
        _, file := filepath.Split(path)

        if (ok) {
            t := time.Now();
            a = append([]interface{} { t.Format("2006-01-02 15:04:05.00"), file, lineno }, a...);
            reason := fmt.Sprintf("%s [%s:%d] " + format + "\n", a...);
            panic(reason);
        } else {
            panic("");
        }
    }
}

func Min(a int, b int) (int) {
    if a < b {
        return a;
    } else {
        return b;
    }
}

func Max(a int, b int) (int) {
    if a > b {
        return a;
    } else {
        return b;
    }
}


