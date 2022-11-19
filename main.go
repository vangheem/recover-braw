package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
)

// Source disk to read from
// SOURCE_DISK = "/dev/sdb"
// var SOURCE_DISK = "/Users/nathan/Downloads/A001_11171838_C008.braw"

var SOURCE_DISK = "/dev/disk5"

// var SOURCE_DISK = "/Users/nathan/Downloads/A001_11171838_C008.braw"

// Target location to save recovered files to
var TARGET_LOCATION = "/Volumes/backup/braw-files"

// var TARGET_LOCATION = "."

// if failed, restart from where you left off
var RESTART_FROM_POSITION = true

// START_STREAM_MATCH = b"\x00\x00\x00\x08wide***\xf8mdat"
var START_STREAM_MATCH = []byte{0, 0, 0, 8, 119, 105, 100, 101, 42, 42, 42, 248, 109, 100, 97, 116}
var START_WAV_MATCH = []byte{0x52, 0x49, 0x46, 0x46, 0x2a, 0x2a, 0x2a, 0x2a, 0x57, 0x41, 0x56, 0x45, 0x66, 0x6d, 0x74, 0x20}
var STAR_BYTE = []byte{0x2a}
var DATA_BYTES = []byte{100, 97, 116, 97}
var NULL_BTYE = []byte{0x0}

var POSITION_FILENAME = "position.go.json"

func human_bytes(num int) string {
	fnum := float64(num)
	for _, unit := range []string{"B", "KB", "MB", "GB", "TB"} {
		if fnum < 1024 {
			return fmt.Sprintf("%.3f %s", fnum, unit)
		}
		fnum /= 1024
	}
	return ""
}

func human_time(seconds int) string {
	hours := seconds / 3600
	seconds = seconds % 3600
	minutes := seconds / 60
	seconds = seconds % 60

	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

type DiskReader struct {
	disk              *os.File
	total_bytes_read  int64
	buffer            []byte
	current_offset    int
	end_buffer_offset int
}

func NewDiskReader(disk_path string) (*DiskReader, error) {
	disk, err := os.Open(disk_path)
	if err != nil {
		return nil, err
	}
	return &DiskReader{disk: disk, total_bytes_read: 0, current_offset: 0, buffer: nil}, nil
}

func (dr *DiskReader) ReadOne() ([]byte, error) {
	if dr.buffer == nil || dr.current_offset >= dr.end_buffer_offset {
		dr.buffer = make([]byte, 1024*1024*5)
		var err error
		dr.end_buffer_offset, err = dr.disk.Read(dr.buffer)
		if err != nil {
			return nil, err
		}
		dr.current_offset = 0
	}
	if dr.end_buffer_offset == 0 {
		return nil, errors.New("end of file")
	}
	dr.total_bytes_read += 1
	output := dr.buffer[dr.current_offset : dr.current_offset+1]
	dr.current_offset += 1
	return output, nil
}

func (dr *DiskReader) Seek(target int64) error {
	dr.total_bytes_read = target
	_, err := dr.disk.Seek(target, 0)
	return err
}

func (dr *DiskReader) Peek(size int) ([]byte, error) {
	err := dr.FillBuffer(size)
	if err != nil {
		return nil, err
	}
	return dr.buffer[dr.current_offset : dr.current_offset+size], nil
}

func (dr *DiskReader) FillBuffer(size int) error {
	if dr.current_offset+size > dr.end_buffer_offset-dr.current_offset {
		buffer := make([]byte, 1024*1024*5)
		num_read, err := dr.disk.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return err
			} else {
				return nil
			}
		}
		dr.buffer = append(dr.buffer, buffer...)
		dr.end_buffer_offset += num_read
	}
	return nil
}

func (dr *DiskReader) Read(size int) ([]byte, error) {
	dr.FillBuffer(size)
	end_offset := dr.current_offset + size
	if end_offset > dr.end_buffer_offset {
		end_offset = dr.end_buffer_offset
	}
	val := dr.buffer[dr.current_offset:end_offset]
	dr.current_offset += size
	dr.total_bytes_read += int64(size)
	return val, nil
}

type FileWriter struct {
	fi                  *os.File
	writer              *bufio.Writer
	filename            string
	total_bytes_written int64
}

func NewFileWriter(filename string) *FileWriter {
	fi, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	return &FileWriter{fi: fi, filename: filename, total_bytes_written: 0, writer: bufio.NewWriter(fi)}
}

func (fr *FileWriter) Write(bytes []byte) error {
	bytes_written, err := fr.writer.Write(bytes)
	if err != nil {
		return err
	}
	fr.total_bytes_written += int64(bytes_written)
	return err
}

func (fr *FileWriter) Close() error {
	fr.writer.Flush()
	err := fr.fi.Close()
	if err != nil {
		return err
	}
	return err
}

type Position struct {
	BytesRead int `json:"bytes_read"`
	FileNum   int `json:"file_num"`
}

func RecordPosition(dr *DiskReader, file_num int) {
	pos := Position{
		BytesRead: int(dr.total_bytes_read),
		FileNum:   file_num,
	}
	bdata, _ := json.Marshal(pos)

	err := os.WriteFile(POSITION_FILENAME, bdata, 0644)
	if err != nil {
		fmt.Println("error writing position file", err, bdata)
	}
}

func ReadPosition() Position {
	if _, err := os.Stat(POSITION_FILENAME); errors.Is(err, os.ErrNotExist) {
		return Position{
			BytesRead: 0,
			FileNum:   0,
		}
	}
	file, _ := ioutil.ReadFile(POSITION_FILENAME)
	var pos Position
	err := json.Unmarshal(file, &pos)
	if err != nil {
		fmt.Println("error reading position file", err, file)
	}
	return pos
}

type RocoveryRunner struct {
	start             time.Time
	last6             []byte
	found_file_num    int
	current_file      *FileWriter
	current_match_pos int
	match_buffer      []byte
	file_reader       *DiskReader

	// test wav file matcher
	wav_current_match_pos int
}

func NewRecoveryRunner() *RocoveryRunner {
	dr, err := NewDiskReader(SOURCE_DISK)
	if err != nil {
		panic(err)
	}
	var found_file_num = 0
	if RESTART_FROM_POSITION {
		pos := ReadPosition()
		found_file_num = pos.FileNum
		dr.Seek(int64(pos.BytesRead))
	}
	return &RocoveryRunner{
		start:             time.Now(),
		last6:             make([]byte, 6),
		found_file_num:    found_file_num,
		current_file:      nil,
		current_match_pos: 0,
		match_buffer:      nil,
		file_reader:       dr,

		wav_current_match_pos: 0,
	}
}

func (rr *RocoveryRunner) Log(msg string, nl bool) {
	var prefix = "[" + human_time(int(time.Since(rr.start).Seconds())) + "] "
	var postfix = "        -----      "
	if nl {
		fmt.Print(prefix + msg + postfix + "\n")
	} else {
		fmt.Print(prefix + msg + postfix + "\r")
	}
}

func (rr *RocoveryRunner) Run() {
	onebyte, err := rr.file_reader.ReadOne()
	for err == nil {
		if rr.file_reader.total_bytes_read%1000000 == 0 {
			rr.Log("Reading "+human_bytes(int(rr.file_reader.total_bytes_read))+" bytes", false)
		}
		if rr.current_file == nil && rr.file_reader.total_bytes_read%10000000 == 0 {
			RecordPosition(rr.file_reader, rr.found_file_num)
		}

		if rr.current_file != nil {
			// write to file
			rr.last6 = append(rr.last6[1:], onebyte...)
			if bytes.Equal(rr.last6[0:1], NULL_BTYE) && bytes.Equal(rr.last6[2:], DATA_BYTES) {
				datum_size := int(rr.last6[1])
				datum, derr := rr.file_reader.Read(datum_size + 3)
				if derr != nil {
					panic(derr)
				}
				rr.Log(fmt.Sprintf("Found datum %d %d %b", datum_size, len(datum), datum), true)
				rr.current_file.Write(onebyte)
				rr.current_file.Write(datum)
				peeked, perr := rr.file_reader.Peek(5)
				if perr != nil {
					panic(perr)
				}
				if !bytes.Equal(peeked[1:], DATA_BYTES) {
					rr.Log("Found end of file", true)
					rr.current_file.Close()
					rr.current_file = nil
				}
			} else {
				rr.current_file.Write(onebyte)
			}
		} else {
			if bytes.Equal(onebyte, START_STREAM_MATCH[rr.current_match_pos:rr.current_match_pos+1]) || bytes.Equal(START_STREAM_MATCH[rr.current_match_pos:rr.current_match_pos+1], STAR_BYTE) {
				// match file start
				rr.current_match_pos += 1
				if rr.match_buffer == nil {
					rr.match_buffer = make([]byte, 0)
				}
				rr.match_buffer = append(rr.match_buffer, onebyte...)
				if rr.current_match_pos == len(START_STREAM_MATCH) {
					rr.found_file_num += 1
					rr.current_match_pos = 0

					filename := fmt.Sprintf("%s/found-file-go-%d.braw", TARGET_LOCATION, rr.found_file_num)
					rr.current_file = NewFileWriter(filename)
					rr.current_file.Write(rr.match_buffer)
					rr.Log("Found file! starting write out "+filename, true)
					rr.match_buffer = nil
				}
			} else {
				rr.current_match_pos = 0
				rr.match_buffer = nil
			}
		}

		if bytes.Equal(onebyte, START_WAV_MATCH[rr.wav_current_match_pos:rr.wav_current_match_pos+1]) || bytes.Equal(START_WAV_MATCH[rr.wav_current_match_pos:rr.wav_current_match_pos+1], STAR_BYTE) {
			rr.wav_current_match_pos += 1
			if rr.wav_current_match_pos == len(START_WAV_MATCH) {
				rr.wav_current_match_pos = 0
				rr.Log("\nFound wav file!", true)
			}
		} else {
			rr.wav_current_match_pos = 0
		}

		onebyte, err = rr.file_reader.ReadOne()
	}
}

func main() {
	fmt.Println("Running data recovery!")

	// defer profile.Start(profile.ProfilePath(".")).Stop()

	rr := NewRecoveryRunner()
	rr.Run()

}
