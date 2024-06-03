package bitcask

import (
	"os"
	"encoding/binary"
	"hash/crc32"
)

type BitcaskScanner struct {
	fileName string
	bufferOffset int
	buf []byte
}

type fileEntry struct {
	crc uint32
	tstamp uint32
	keySize uint32
	valSize uint32
	key string
	val string
}


func NewBitcaskScanner(fileName string) *BitcaskScanner {
	b, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	return &BitcaskScanner{
		fileName: fileName,
		bufferOffset: 0,
		buf: b,
	}
} 

// Returns true if there are more bytes to read
func (scanner *BitcaskScanner) scan() bool {
	if scanner.bufferOffset >= len(scanner.buf) {
		return false
	}
	return true
}

// Seeks to a specific offset in buffer
func (scanner *BitcaskScanner) seek(offset int) *fileEntry {
	prevOffset := scanner.bufferOffset
	scanner.bufferOffset = offset
	result := scanner.next()
	scanner.bufferOffset = prevOffset
	return result
}

func (scanner *BitcaskScanner) retrieveValue(valueSize int, valuePos int) (string) {
	return string(scanner.buf[valuePos:valuePos+valueSize])
}

func (scanner *BitcaskScanner) next() *fileEntry {
	o := scanner.bufferOffset

	// Grab main data
	crc := binary.LittleEndian.Uint32(scanner.buf[o:o+4])
	tstamp := binary.LittleEndian.Uint32(scanner.buf[o+4:o+8])
	keySize := binary.LittleEndian.Uint32(scanner.buf[o+8:o+12])
	valSize := binary.LittleEndian.Uint32(scanner.buf[o+12:o+16])
	key := string(scanner.buf[o+16:o+int(keySize)+16])

	// Check if tombstone
	if valSize == 0 {
		scanner.bufferOffset += 16 + int(keySize)
		return &fileEntry {
			crc: crc,
			tstamp: tstamp,
			keySize: keySize,
			valSize: valSize,
			key: key,
			val: "",
		}
	}
	
	val := string(scanner.buf[o+int(keySize)+16:o+int(keySize)+16+int(valSize)])

	// Check checksum
	if crc32.ChecksumIEEE(scanner.buf[o+4:o+int(valSize)+int(keySize)+16]) != crc {
		panic("Checksum mismatch")
	}

	scanner.bufferOffset += 16 + int(keySize) + int(valSize)
	return &fileEntry {
		crc: crc,
		tstamp: tstamp,
		keySize: keySize,
		valSize: valSize,
		key: key,
		val: val,
	}
}

