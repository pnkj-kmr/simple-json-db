package simplejsondb

import (
	"bytes"
	"compress/gzip"
	"io"
)

func UnGzip(record []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	_, err = buffer.Write(record)
	if err != nil {
		return record, err
	}
	reader, err := gzip.NewReader(&buffer)

	result, err = io.ReadAll(reader)
	if err != nil {
		return record, err
	}

	err = reader.Close()
	if err != nil {
		return record, nil
	}

	return
}

func Gzip(data []byte) (result []byte, err error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err = writer.Write(data)
	if err != nil {
		return data, err
	}
	err = writer.Close()
	result = buffer.Bytes()
	return result, err
}
