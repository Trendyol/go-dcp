# go-dcp-client

### example usage

```go
package main

import (
	"github.com/couchbase/gocbcore/v10"
	"log"
)

func listener(event int, data interface{}, err error) {
	if err != nil {
		return
	}

	if event == MutationName {
		mutation := data.(gocbcore.DcpMutation)
		log.Printf("mutated | id: %v, value: %v", string(mutation.Key), string(mutation.Value))
	} else if event == DeletionName {
		deletion := data.(gocbcore.DcpDeletion)
		log.Printf("deleted | id: %v, value: %v", string(deletion.Key), string(deletion.Value))
	} else if event == ExpirationName {
		expiration := data.(gocbcore.DcpExpiration)
		log.Printf("expired | id: %v", string(expiration.Key))
	}
}

func main() {
	dcp, err := NewDcp("configs/main.yml", listener)
	defer dcp.Close()

	if err != nil {
		panic(err)
	}

	dcp.StartAndWait()
}
```