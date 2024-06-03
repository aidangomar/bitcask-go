package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"github.com/google/uuid"
)

func serializeKV(key string, value string, time uint32) []byte {
	// Write main data to buffer
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, time)
	binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	binary.Write(buf, binary.LittleEndian, []byte(key))
	binary.Write(buf, binary.LittleEndian, []byte(value))

	// Calculate CRC32 checksum
	hashBuf := new(bytes.Buffer)
	binary.Write(hashBuf, binary.LittleEndian, crc32.ChecksumIEEE(buf.Bytes()))
	
	// Combine buffers
	hashBuf.Write(buf.Bytes())
	
	return hashBuf.Bytes()
}

// Writes to active datafile. Post-write, if file exceeds threshold,
// function will create a new datafile, update Bitcask accordingly, and write to the new file
func (bitcask *Bitcask) writeToActiveDatafile(key string, val string, time uint32) error {
	// If datafile doesn't exist, create it
	if _, err := os.Stat(bitcask.activeFile); os.IsNotExist(err) {
		file, err := os.Create(bitcask.activeFile)
		if err != nil {
			return err
		}
		file.Close()
	}

	// Open active datafile
	file, err := os.OpenFile(bitcask.activeFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	// Serialize entry
	bytes := serializeKV(key, val, time)

	// Write to the datafile
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}
	
	// Check if file exceeds size threshold
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() < bitcask.sizeThreshold {
		return nil
	}
	
	// Create new file if threshold is exceeded
	fileName := bitcask.directoryName+"/"+uuid.New().String()
	_, err = os.Create(fileName)
	if err != nil {
		return err
	}
	bitcask.activeFile = fileName
	return nil
}
