package bitcask

import (
	"errors"
	"os"
	// "sync"
)

type Bitcask struct {

	// Permissions
	read bool
	write bool
	create bool
	syncOnPut bool

	// File information
	directoryName string
	activeFile string
	sizeThreshold int64

	// Keydir information
	keydir map[string]keydirEntry

	// Concurrency TODO
	// mutex sync.Mutex
}

// File permissions
const (
	READ 	= 1 << iota
	WRITE
	CREATE
	SYNC_ON_PUT
)

// Parses the options passed to Open. Opts in this case is a list containing a single integer
// representing the permissions of the Bitcask instance given by the user
func (bitcask *Bitcask) addPermissions(opts ...int) (error) {
	if len(opts) == 0 {
		bitcask.read = true
		return nil
	}
	if len(opts) > 1 {
		return errors.New("Options must be a single argument or nonexistent (Ex. bitcask.READ|bitcask.WRITE|bitcask.CREATE)")
	}
	if opts[0] & READ != 0 {
		bitcask.read = true
	}
	if opts[0] & WRITE != 0 {
		bitcask.write = true
	}
	if opts[0] & CREATE != 0 {
		bitcask.create = true
	}
	if opts[0] & SYNC_ON_PUT != 0 {
		bitcask.syncOnPut = true
	}
	return nil
}

// Open a new or existing Bitcask datastore with additional options. 
// Valid options are bitcask.READ, bitcask.WRITE, bitcask.CREATE, and bitcask.SYNC_ON_PUT
// TODO "Only one process may open a Bitcask with read_write at a time"
func Open(directoryName string, opts ...int) (*Bitcask, error) {
	bitcask := &Bitcask{}

	// Add permissions to Bitcask based on user
	if err := bitcask.addPermissions(opts...); err != nil {
		return nil, err
	}
	
	// Create bitcask directory if applicable
	if _, err := os.Stat(directoryName); os.IsNotExist(err) && !bitcask.create {
		return nil, errors.New("Directory does not exist")
	}
	if _, err := os.Stat(directoryName); os.IsNotExist(err) && bitcask.create {
		err := os.Mkdir(directoryName, 0755)
		if err != nil {
			return nil, err
		}
	}
	bitcask.directoryName = directoryName
	bitcask.sizeThreshold = 200000
	
	// Initialize Keydir
	bitcask.init()

	return bitcask, nil
}
