package csv

import (
	"encoding/csv"
	"io"
	"os"
)

func Read(fileName string, skipHeader bool, readFunc func(int, []string) error) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	if skipHeader {
		r.Read()
	}
	i := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		i++
		if err := readFunc(i, record); err != nil {
			return err
		}
	}
	return nil
}

func ReadByChunk(fileName string, chunkSize int, skipHeader bool, readByChunkFunc func([][]string) error) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	var records [][]string
	if skipHeader {
		r.Read()
	}
	i := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		records = append(records, record)
		i++
		if i%chunkSize == 0 {
			if err := readByChunkFunc(records); err != nil {
				return err
			}
			records = [][]string{}
		}
	}
	if err := readByChunkFunc(records); err != nil {
		return err
	}
	return nil
}
