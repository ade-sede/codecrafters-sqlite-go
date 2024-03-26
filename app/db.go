package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type TableType uint8

const (
	INTERIOR_INDEX TableType = 0x02
	INTERIOR_TABLE TableType = 0x05
	LEAF_INDEX     TableType = 0x0A
	LEAF_TABLE     TableType = 0x0D
)

type SerialType int64

const (
	NULL               SerialType = 0x00
	INT8               SerialType = 0x01
	BIG_ENDIAN_INT16   SerialType = 0x02
	BIG_ENDIAN_INT24   SerialType = 0x03
	BIG_ENDIAN_INT32   SerialType = 0x04
	BIG_ENDIAN_INT48   SerialType = 0x05
	BIG_ENDIAN_INT64   SerialType = 0x06
	BIG_ENDIAN_FLOAT64 SerialType = 0x07
	VALUE_0            SerialType = 0x08
	VALUE_1            SerialType = 0x09
	RESERVED_1         SerialType = 0x0A
	RESERVED_2         SerialType = 0x0B
)

type Record struct {
	payload    []byte
	serialType SerialType
}

type Cell struct {
	Type              TableType
	rowId             int64
	payloadSize       int64
	payloadHeader     []byte
	payloadBody       []byte
	pointerToLeafPage uint32
}

type Page struct {
	file                  *os.File
	Type                  TableType
	pageStart             uint
	pageSize              uint16
	cellPointerOffset     uint
	cellCount             uint16
	cellContentAreaOffset uint16
	rightMostPointer      uint32
}

type Database struct {
	file     *os.File
	PageSize uint16
}

func NewDBHandler(filepath string) (*Database, error) {
	var pageSize uint16

	file, err := os.Open(filepath)

	if err != nil {
		return nil, err
	}

	header := make([]byte, 100)

	_, err = file.Read(header)

	if err != nil {
		return nil, err
	}

	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize)

	if err != nil {
		return nil, err
	}

	database := Database{
		PageSize: pageSize,
		file:     file,
	}

	return &database, nil
}

func (d *Database) Close() error {
	return d.file.Close()
}

// First page is `pageNumber` 0.
func (d *Database) getPage(pageNumber int) (*Page, error) {
	page := Page{
		file:                  d.file,
		pageSize:              d.PageSize,
		Type:                  INTERIOR_INDEX,
		pageStart:             0,
		cellPointerOffset:     0,
		cellCount:             0,
		cellContentAreaOffset: 0,
		rightMostPointer:      0,
	}

	var header []byte
	var headerOffset uint

	header = make([]byte, 12)
	page.pageStart = uint(pageNumber) * uint(d.PageSize)
	headerOffset = 0

	if pageNumber == 0 {
		headerOffset = 100
	}

	_, err := d.file.Seek(int64(page.pageStart)+int64(headerOffset), io.SeekStart)

	if err != nil {
		return nil, err
	}

	_, err = d.file.Read(header)

	if err != nil {
		return nil, err
	}

	page.Type = TableType(header[0])

	if page.Type != LEAF_TABLE && page.Type != INTERIOR_TABLE {
		return nil, fmt.Errorf("Unsupported page type: %v", page.Type)
	}

	err = binary.Read(bytes.NewReader(header[3:5]), binary.BigEndian, &page.cellCount)

	if err != nil {
		return nil, err
	}

	err = binary.Read(bytes.NewReader(header[5:7]), binary.BigEndian, &page.cellContentAreaOffset)

	if err != nil {
		return nil, err
	}

	page.cellPointerOffset = uint(headerOffset) + 8

	if page.Type == INTERIOR_TABLE {
		err = binary.Read(bytes.NewReader(header[8:12]), binary.BigEndian, &page.rightMostPointer)

		if err != nil {
			return nil, err
		}

		page.cellPointerOffset += 4
	}

	return &page, nil
}

func leafTableCells(p *Page, cellContents []byte) ([]*Cell, error) {
	cells := make([]*Cell, p.cellCount)

	for i := 0; i < int(p.cellCount); i++ {
		payloadSize, n := readBigEndianVarint(cellContents)

		if n <= 0 {
			return nil, errors.New("Error while reading varint, n <= 0")
		}

		cellContents = cellContents[n:]

		rowId, n := readBigEndianVarint(cellContents)

		if n <= 0 {
			return nil, errors.New("Error while reading varint, n <= 0")
		}

		cellContents = cellContents[n:]

		payloadHeaderSize, n := readBigEndianVarint(cellContents)

		if n <= 0 {
			return nil, errors.New("Error while reading varint, n <= 0")
		}

		payloadBodySize := payloadSize - payloadHeaderSize

		payloadHeader := cellContents[:payloadHeaderSize]
		cellContents = cellContents[payloadHeaderSize:]

		payloadBody := cellContents[:payloadBodySize]
		cellContents = cellContents[payloadBodySize:]

		cell := Cell{
			payloadSize:   payloadSize,
			payloadBody:   payloadBody,
			payloadHeader: payloadHeader,
			rowId:         rowId,
		}

		cells[i] = &cell
	}

	return cells, nil
}

func interiorTableCells(p *Page, cellContents []byte) ([]*Cell, error) {
	cells := make([]*Cell, p.cellCount)

	for i := 0; i < int(p.cellCount); i++ {
		var pageNumber uint32

		err := binary.Read(bytes.NewReader(cellContents), binary.BigEndian, &pageNumber)

		if err != nil {
			return nil, err
		}

		cellContents = cellContents[4:]

		rowId, n := readBigEndianVarint(cellContents)

		if n <= 0 {
			return nil, errors.New("Error while reading varint, n <= 0")
		}

		cellContents = cellContents[n:]

		cell := Cell{
			Type:              p.Type,
			rowId:             rowId,
			pointerToLeafPage: pageNumber,
		}

		cells[i] = &cell
	}

	return cells, nil
}

func (p *Page) cells() ([]*Cell, error) {
	// TODO: take reserved empty bytes into account
	startOfCellContent := p.pageStart + uint(p.cellContentAreaOffset)
	endOfPage := p.pageStart + uint(p.pageSize)

	cellContents := make([]byte, endOfPage-uint(startOfCellContent))

	_, err := p.file.Seek(int64(startOfCellContent), io.SeekStart)

	if err != nil {
		return nil, err
	}

	_, err = p.file.Read(cellContents)

	if err != nil {
		return nil, err
	}

	if p.Type == LEAF_TABLE {
		return leafTableCells(p, cellContents)
	} else if p.Type == INTERIOR_TABLE {
		return interiorTableCells(p, cellContents)
	}

	return nil, fmt.Errorf("Unsupported page type: %v", p.Type)
}

func decodePayload(header []byte, payload []byte) ([]*Record, error) {
	_, n := readBigEndianVarint(header)

	if n <= 0 {
		return nil, errors.New("Error while reading varint, n <= 0")
	}

	header = header[n:]

	records := make([]*Record, 0)

	for {
		if len(header) == 0 {
			break
		}

		serialType, n := readBigEndianVarint(header)

		if n <= 0 {
			return nil, errors.New("Error while reading varint, n <= 0")
		}

		header = header[n:]

		if SerialType(serialType) == INT8 {
			field := payload[0:]
			payload = payload[1:]

			records = append(records, &Record{
				payload:    field,
				serialType: SerialType(serialType),
			})
		} else if SerialType(serialType) == BIG_ENDIAN_INT16 {
			field := payload[0:2]
			payload = payload[2:]

			records = append(records, &Record{
				payload:    field,
				serialType: SerialType(serialType),
			})
		} else if SerialType(serialType) == NULL {
			field := payload[0:]

			records = append(records, &Record{
				payload:    field,
				serialType: SerialType(serialType),
			})
		} else if SerialType(serialType) == VALUE_1 {
			field := payload[0:]

			records = append(records, &Record{
				payload:    field,
				serialType: SerialType(serialType),
			})
		} else if serialType >= 13 && serialType&0b1 == 1 {
			fieldLen := (serialType - 13) / 2
			field := payload[:fieldLen]
			payload = payload[fieldLen:]

			records = append(records, &Record{
				payload:    field,
				serialType: SerialType(serialType),
			})

		} else {
			return nil, fmt.Errorf("Unsupported serialType: %v", serialType)
		}

	}

	return records, nil
}

func readBigEndianVarint(data []byte) (int64, int) {
	var value int64
	var bytesRead int

	for index, b := range data {
		value = (value << 7) | int64(b&0x7F)
		bytesRead++

		if b&0x80 == 0 {
			return value, bytesRead
		}

		if index > 9 {
			return 0, -bytesRead
		}
	}

	return 0, 0
}
