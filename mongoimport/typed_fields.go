package mongoimport

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/mongoimport/dateconv"
)

// columnType defines different types for columns that can be parsed distinctly
type columnType int

const (
	CT_AUTO columnType = iota
	CT_BINARY
	CT_BOOLEAN
	CT_DATE
	CT_DATE_GO
	CT_DATE_MS
	CT_DATE_ORACLE
	CT_DOUBLE
	CT_INT_32
	CT_INT_64
	CT_STRING
)

var (
	columnTypeRE      = regexp.MustCompile(`(?s)^(.*)\.(\w+)\((.*)\)$`)
	columnTypeNameMap = map[string]columnType{
		"auto":        CT_AUTO,
		"binary":      CT_BINARY,
		"boolean":     CT_BOOLEAN,
		"date":        CT_DATE,
		"date_go":     CT_DATE_GO,
		"date_ms":     CT_DATE_MS,
		"date_oracle": CT_DATE_ORACLE,
		"double":      CT_DOUBLE,
		"int32":       CT_INT_32,
		"int64":       CT_INT_64,
		"string":      CT_STRING,
	}
)

type binaryEncoding int

const (
	BE_BASE_64 binaryEncoding = iota
	BE_BASE_32
	BE_HEX
)

var binaryEncodingNameMap = map[string]binaryEncoding{
	"base64": BE_BASE_64,
	"base32": BE_BASE_32,
	"hex":    BE_HEX,
}

// ColumnSpec keeps information for each 'column' of import.
type ColumnSpec struct {
	Name       string
	Parser     FieldParser
	ParseGrace ParseGrace
}

// ColumnNames maps a ColumnSpec slice to their associated names
func ColumnNames(fs []ColumnSpec) (s []string) {
	for _, f := range fs {
		s = append(s, f.Name)
	}
	return
}

// ParseTypedHeader produces a ColumnSpec from a header item, extracting type
// information from the it. The parseGrace is passed along to the new ColumnSpec.
func ParseTypedHeader(header string, parseGrace ParseGrace) (f ColumnSpec, err error) {
	match := columnTypeRE.FindStringSubmatch(header)
	if len(match) != 4 {
		err = fmt.Errorf("could not parse type from header %s", header)
		return
	}
	t, ok := columnTypeNameMap[match[2]]
	if !ok {
		err = fmt.Errorf("invalid type %s in header %s", match[2], header)
		return
	}
	p, err := NewFieldParser(t, match[3])
	if err != nil {
		return
	}
	return ColumnSpec{match[1], p, parseGrace}, nil
}

// ParseTypedHeaders performs ParseTypedHeader on each item, returning an
// error if any single one fails.
func ParseTypedHeaders(headers []string, parseGrace ParseGrace) (fs []ColumnSpec, err error) {
	fs = make([]ColumnSpec, len(headers))
	for i, f := range headers {
		fs[i], err = ParseTypedHeader(f, parseGrace)
		if err != nil {
			return
		}
	}
	return
}

// ParseAutoHeaders converts a list of header items to ColumnSpec objects, with
// automatic parsers.
func ParseAutoHeaders(headers []string) (fs []ColumnSpec) {
	fs = make([]ColumnSpec, len(headers))
	for i, f := range headers {
		fs[i] = ColumnSpec{f, new(FieldAutoParser), PG_AUTO_CAST}
	}
	return
}

// FieldParser is the interface for any parser of a field item.
type FieldParser interface {
	Parse(in string) (interface{}, error)
}

var (
	escapeReplacements = []string{
		`\\`, `\`,
		`\(`, `(`,
		`\)`, `)`,
	}
	escapeReplacer = strings.NewReplacer(escapeReplacements...)
)

// NewFieldParser yields a FieldParser corresponding to the given columnType.
// arg is passed along to the specific type's parser, if it permits an
// argument. An error will be raised if arg is not valid for the type's
// parser.
func NewFieldParser(t columnType, arg string) (parser FieldParser, err error) {
	arg = escapeReplacer.Replace(arg)

	switch t { // validate argument
	case CT_BINARY:
	case CT_DATE:
	case CT_DATE_GO:
	case CT_DATE_MS:
	case CT_DATE_ORACLE:
	default:
		if arg != "" {
			err = fmt.Errorf("type %v does not support arguments", t)
			return
		}
	}

	switch t {
	case CT_BINARY:
		parser, err = NewFieldBinaryParser(arg)
	case CT_BOOLEAN:
		parser = new(FieldBooleanParser)
	case CT_DATE:
		fallthrough
	case CT_DATE_GO:
		parser = &FieldDateParser{arg}
	case CT_DATE_MS:
		parser = &FieldDateParser{dateconv.FromMS(arg)}
	case CT_DATE_ORACLE:
		parser = &FieldDateParser{dateconv.FromOracle(arg)}
	case CT_DOUBLE:
		parser = new(FieldDoubleParser)
	case CT_INT_32:
		parser = new(FieldInt32Parser)
	case CT_INT_64:
		parser = new(FieldInt64Parser)
	case CT_STRING:
		parser = new(FieldStringParser)
	default: // CT_AUTO
		parser = new(FieldAutoParser)
	}
	return
}

func autoParse(in string) interface{} {
	parsedInt, err := strconv.Atoi(in)
	if err == nil {
		return parsedInt
	}
	parsedFloat, err := strconv.ParseFloat(in, 64)
	if err == nil {
		return parsedFloat
	}
	return in
}

type FieldAutoParser struct{}

func (ap *FieldAutoParser) Parse(in string) (interface{}, error) {
	return autoParse(in), nil
}

type FieldBinaryParser struct {
	enc binaryEncoding
}

func (bp *FieldBinaryParser) Parse(in string) (interface{}, error) {
	switch bp.enc {
	case BE_BASE_32:
		return base32.StdEncoding.DecodeString(in)
	case BE_BASE_64:
		return base64.StdEncoding.DecodeString(in)
	default: // BE_HEX
		return hex.DecodeString(in)
	}
}

func NewFieldBinaryParser(arg string) (*FieldBinaryParser, error) {
	enc, ok := binaryEncodingNameMap[arg]
	if !ok {
		return nil, fmt.Errorf("invalid binary encoding: %s", arg)
	}
	return &FieldBinaryParser{enc}, nil
}

type FieldBooleanParser struct{}

func (bp *FieldBooleanParser) Parse(in string) (interface{}, error) {
	if strings.ToLower(in) == "true" || in == "1" {
		return true, nil
	}
	if strings.ToLower(in) == "false" || in == "0" {
		return false, nil
	}
	return nil, fmt.Errorf("failed to parse boolean: %s", in)
}

type FieldDateParser struct {
	layout string
}

func (dp *FieldDateParser) Parse(in string) (interface{}, error) {
	return time.Parse(dp.layout, in)
}

type FieldDoubleParser struct{}

func (dp *FieldDoubleParser) Parse(in string) (interface{}, error) {
	return strconv.ParseFloat(in, 64)
}

type FieldInt32Parser struct{}

func (ip *FieldInt32Parser) Parse(in string) (interface{}, error) {
	value, err := strconv.ParseInt(in, 10, 32)
	return int32(value), err
}

type FieldInt64Parser struct{}

func (ip *FieldInt64Parser) Parse(in string) (interface{}, error) {
	return strconv.ParseInt(in, 10, 64)
}

type FieldStringParser struct{}

func (sp *FieldStringParser) Parse(in string) (interface{}, error) {
	return in, nil
}
