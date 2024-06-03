# Go Bitcask Storage Engine

A Go implementation of the bitcask storage engine (https://riak.com/assets/bitcask-intro.pdf) used in Riak KV

## Implementation 
Bitcask is a log-structured hash table for fast key/value data. Similar to LSM Trees, Bitcask keeps log files of all 
datafiles and performs merging when needed to reduce read latency. Bitcask's advantages come from it's in-memory key directory, which caches the file\_id of each key, such that every read only requires at most one disk seek.

## Usage
Import the package

```go
import (
    ...
    "bitcask-go/internal/bitcask"
    ...
)

...

db, err := bitcask.Open("bitcask-directory", bitcask.READ|bitcask.WRITE|bitcask.CREATE)
```

## API

### Open
- Opens database for usage. If database does not exist and WRITE flag is passed, a new bitcask will be created

```
db, err := bitcask.Open("new-bitcask-store", bitcask.READ|bitcask.WRITE|bitcask.CREATE)
```

### Put
- Inserts a kv pair into the database. Automatically syncs to disk
```
err := db.Put("key", "value")
```

### Get
- Retrieves a value from the database
```
value, err := db.Get("key")
```

### Delete
- Deletes a value from the database. Automatically syncs to disk
```
err := db.Delete("key")
```

### Merge
- Compacts and merges datafiles on disk to cull tombstones and old entries
```
err := db.Merge()
```
