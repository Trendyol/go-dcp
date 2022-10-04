# go-dcp-client

```go

 	InitFromYml("config/test.yml", func(mutation Mutation) {
            println("mutated %s", mutation.Key)
        }, func(deletion Deletion) {
            println("Deleted %s", deletion.Key)
        }, nil)

    // or

        Init(config.CouchbaseDCPConfig{
        //  config fields here
        }, func(mutation Mutation) {
            println("mutated %s", mutation.Key)
        }, func(deletion Deletion) {
            println("Deleted %s", deletion.Key)
        }, nil)
 ```