package mr

import (
	"crypto/sha1"
	"encoding/hex"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"
)

type State uint8
const (
	Spare State = iota
	Mapping
	Reducing
	Done
)
var str = []string{ "Spare", "Mapping", "Reducing", "Done" }
var mSock string

const (
	deltaTime = 200 * time.Millisecond
	workerTimeout = 10 * time.Second
	masterLifetime = 3 * time.Minute
)

func init() {
	mSock = getRandomString(16)
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)
	//pwd, _ := os.Getwd()
	//fname := fmt.Sprintf("%v/log", pwd)
	//os.Remove(fname)
	//f, _ := os.Create(fname)
	//log.SetOutput(f)
}

// impl String interface
func (s State) String() string {
	return str[s]
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	return "/var/tmp/824-mr-" + strconv.Itoa(os.Getuid())
}

func workerSock(workerID string) string {
	return "/var/tmp/824-mr-" + workerID
}

func genWorkerID() string {
	rand.Seed(time.Now().UnixNano())
	return getRandomString(16)
}

func getTaskID(filename string) string {
	h := sha1.New()
	h.Write([]byte(filename) )
	ret := h.Sum(nil)
	return hex.EncodeToString(ret)
}

func getRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func removeAll(dir string) {
	filenames, _ := ioutil.ReadDir(dir)
	for _, d := range filenames {
		if err := os.RemoveAll(path.Join([]string{dir, d.Name()}...) ); err != nil {
			log.Printf("removing exist filenames in %s: %v\n", )
		}
	}
}


