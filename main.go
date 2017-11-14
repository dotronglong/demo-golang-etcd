package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	"sync"
)

const (
	ETCD_ADDRESS = "http://192.168.33.58:2379/v2/keys"
)

type Node struct {
	Dir           bool   `json:"dir"`
	Nodes         []Node `json:"nodes"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	CreatedIndex  uint64 `json:"createdIndex"`
	ModifiedIndex uint64 `json:"modifiedIndex"`
}

type Response struct {
	Action   string `json:"action"`
	Node     Node   `json:"node"`
	PrevNode Node   `json:"prevNode"`
}

var (
	writeCount  = flag.Int("writeCount", 0, "Total of writes count")
	writeKey = flag.String("writeKey", "", "Write a specific key")

	readCount  = flag.Int("readCount", 0, "Total of reads count")
	readKey = flag.String("readKey", "", "Read a specific key")
	readCon = flag.Bool("readCon", false, "Read concurrently")

	action = flag.String("action", "", "Perform an action. Supported: read, write")
	verbose = flag.Bool("verbose", false, "Print debug log")
)

func init() {
	flag.Parse()
}

func main() {
	fmt.Printf("action=%s verbose=%t writeCount=%d writeKey=%s readCount=%d readKey=%s readCon=%t\n", *action, *verbose, *writeCount, *writeKey, *readCount, *readKey, *readCon)
	switch *action {
	case "read":
		if *readCount <= 0 {
			break
		}
		r := get("")
		l := len(r.Node.Nodes)
		start := time.Now()
		if *readCon {
			// Read concurrently
			var wg sync.WaitGroup
			var sem = make(chan struct{}, 100)
			var key string
			for i := 0; i < *readCount; i++ {
				wg.Add(1)
				if *readKey != "" {
					key = *readKey
				} else {
					key = r.Node.Nodes[i % l].Key
				}
				select {
				case sem<- struct{}{}:
					go func(key string) {
						rs := get(key)
						logf("%s => %s\n", rs.Node.Key, rs.Node.Value)
						<-sem
						wg.Done()
					}(key)
				}
			}
			wg.Wait()
		} else {
			// Normal read
			var key string
			for i := 0; i < *readCount; i++ {
				if *readKey != "" {
					key = *readKey
				} else {
					key = r.Node.Nodes[i % l].Key
				}
				rs := get(key)
				logf("%s => %s\n", rs.Node.Key, rs.Node.Value)
			}
		}
		fmt.Printf("Processed %d in %s\n", *readCount, time.Since(start))
		break
	case "write":
		if *writeCount <= 0 {
			break
		}
		var key string
		start := time.Now()
		for i := 0; i < *writeCount; i++ {
			if *writeKey != "" {
				key = *writeKey
			} else {
				key = fmt.Sprintf("%d", time.Now().Unix())
			}
			put(key, fmt.Sprintf("%d", i))
		}
		fmt.Printf("Processed %d in %s\n", *writeCount, time.Since(start))
		break
	default:
		break
	}
}

func get(key string) *Response {
	response, err := http.Get(fmt.Sprintf("%s/%s", ETCD_ADDRESS, key))
	if err != nil {
		panic(err)
	}

	return parse(response)
}

func put(key, value string) *Response {
	payload := strings.NewReader(fmt.Sprintf("value=%s", value))
	request, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", ETCD_ADDRESS, key), payload)
	if err != nil {
		panic(err)
	}

	request.Header.Add("content-type", "application/x-www-form-urlencoded")
	request.Header.Add("cache-control", "no-cache")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		panic(err)
	}

	return parse(response)
}

func parse(response *http.Response) *Response {
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	r := new(Response)
	if err := json.Unmarshal(body, r); err != nil {
		panic(err)
	}

	return r
}

func logf(format string, args ...interface{}) {
	if *verbose {
		fmt.Printf(format, args...)
	}
}