package bitcask

import (
	"errors"
	"github.com/google/uuid"
	"os"
	"time"
)

type keydirEntry struct {
	file_id  string
	value_sz uint32
	value_pos uint64
	tstamp   uint32
}

// Populates keydir
func (bitcask *Bitcask) init() (error) {
	bitcask.keydir = make(map[string]keydirEntry)

	files, err := os.ReadDir(bitcask.directoryName)
	if err != nil {
		return err
	}	

	// If no files exist, create new active file
	// Otherwise find the file with the least entries
	if len(files) == 0 {
		bitcask.activeFile = bitcask.directoryName+"/"+uuid.New().String()
	} else {
		var min os.FileInfo
		for _, file := range files {
			fileName := file.Name()
			f, err := os.Stat(fileName)
			if err != nil {
				return err
			}
			if f.Size() < min.Size() {
				bitcask.activeFile = bitcask.directoryName+"/"+fileName
				min = f
			}
		}
	}

		
	// Populate keydir with entries from all datafiles
	for _, file := range files {
		scanner := NewBitcaskScanner(file.Name())
		for scanner.scan() {
			entry := scanner.next()
			// If key doesn't exist in keydir or if the entry is newer, update keydir
			if _, exists := bitcask.keydir[entry.key]; !exists || entry.tstamp > bitcask.keydir[entry.key].tstamp {
				bitcask.keydir[entry.key] = keydirEntry{
					file_id:  file.Name(),
					value_sz: entry.valSize,
					tstamp:   entry.tstamp,
				}
			}
		}
	}

	// Remove tombstones from keydir
	for key, entry := range bitcask.keydir {
		if entry.value_sz == 0 {
			delete(bitcask.keydir, key)
		}
	}
	return nil
}


// Writes kv pair to database and commits entry to keydir
func (bitcask *Bitcask) Put(key string, value string) (err error) {
	if !bitcask.write {
		return errors.New("Bitcask instance is not writeable")
	}	

	// Create tstamp for write
	time := uint32(time.Now().Unix())

	// Write to active datafile
	err = bitcask.writeToActiveDatafile(key, value, time)
	if err != nil {
		return err
	}

	// Compute valuePos
	file, err := os.Stat(bitcask.activeFile)
	if err != nil {
		return err
	}
	valuePos := uint64(file.Size()) - uint64(len(value))

	// Write to the keydir
	if value != "" {
		bitcask.keydir[key] = keydirEntry{
			file_id:  bitcask.activeFile,
			value_sz: uint32(len(value)),
			value_pos: valuePos, 
			tstamp:   time,
		}
	}
	return err
}


func (bitcask *Bitcask) Delete(key string) (err error) {
	if !bitcask.write {
		return errors.New("Bitcask instance is not writeable")
	}

	// Check if key doesn't exist
	_, exists := bitcask.keydir[key]
	if !exists {
		return errors.New("Key not found")
	}

	// Create tombstone
	err = bitcask.Put(key, "")
	if err != nil {
		return err
	}

	// Remove key from keydir
	delete(bitcask.keydir, key)
	return nil
}


// Returns the latest value for a given key
func (bitcask *Bitcask) Get(key string) (string, error) {
	if !bitcask.read {
		return "", errors.New("Bitcask instance is not readable")
	}

	kd_entry, exists := bitcask.keydir[key]

	// If key doesn't exist in keydir or tombstone, return err
	if !exists || kd_entry.value_sz == 0 {
		return "", errors.New("Key not found")
	}
	
	// Otherwise, jump to offset in datafile and get value
	f, err := os.Open(kd_entry.file_id)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// call scanner
	scanner := NewBitcaskScanner(kd_entry.file_id)
	return scanner.retrieveValue(int(kd_entry.value_sz), int(kd_entry.value_pos)), nil
}

// Loops through all datafiles and combines valid entries into new files.
// Removes tombstones from keydir
func (bitcask *Bitcask) Merge() error {
	files, err := os.ReadDir(bitcask.directoryName)
	if err != nil {
		return err
	}	

	// Create new file for merge
	newFile, err := os.Create(bitcask.directoryName+"/"+uuid.New().String())
	if err != nil {
		return err
	}
	defer newFile.Close()

	for _, file := range files {
		scanner := NewBitcaskScanner(bitcask.directoryName+"/"+file.Name())
		for scanner.scan() {
			entry := scanner.next()
			kd_entry, exists := bitcask.keydir[entry.key]

			// Check if key exists in keydir and it's the most recent entry
			if exists && kd_entry.tstamp == entry.tstamp {
				// Write to new file
				newFile.Write(serializeKV(entry.key, entry.val, entry.tstamp))

				// Update keydir
				f, err := os.Stat(newFile.Name())				
				if err != nil {
					return err
				}
				bitcask.keydir[entry.key] = keydirEntry{
					file_id:  newFile.Name(),
					value_sz: entry.valSize,
					value_pos: uint64(f.Size()) - uint64(len(entry.val)),
					tstamp:   entry.tstamp,
				}
			}
			// Create another merge file if newFile exceeds size
			f, err := os.Stat(newFile.Name())
			if err != nil {
				return err
			}
			if f.Size() > bitcask.sizeThreshold {
				newFile, err = os.Create(bitcask.directoryName+"/"+uuid.New().String())
				if err != nil {
					return err
				}
				defer newFile.Close()
			}
		}
		// Remove old file
		os.Remove(bitcask.directoryName+"/"+file.Name())
	}

	// Remove tombstones from keydir
	for key, entry := range bitcask.keydir {
		if entry.value_sz == 0 {
			delete(bitcask.keydir, key)
		}
	}
	return nil
}

