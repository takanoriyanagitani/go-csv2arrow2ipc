package csv2ipc

import (
	"io"
	"iter"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	ac "github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type CsvReader struct {
	reader *ac.Reader
	schema *arrow.Schema
	first  arrow.RecordBatch
	used   bool
}

func (r *CsvReader) Schema() *arrow.Schema {
	if r.schema == nil {
		if r.reader.Next() {
			r.first = r.reader.Record()
			r.first.Retain()
			r.schema = r.first.Schema()
		}
	}
	return r.schema
}

func (r *CsvReader) Next() bool {
	return r.reader.Next()
}

func (r *CsvReader) Record() arrow.RecordBatch {
	return r.reader.Record()
}

func (r *CsvReader) Err() error {
	return r.reader.Err()
}

func (r *CsvReader) ToIter() iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		if r.first != nil && !r.used {
			r.used = true
			if !yield(r.first, nil) {
				r.first.Release()
				return
			}
			r.first.Release()
		}

		it := array.IterFromReader(r.reader)
		for b, err := range it {
			if !yield(b, err) {
				if b != nil {
					b.Release()
				}
				return
			}
		}
	}
}

type CsvReaderOpts struct {
	Opts []ac.Option
}

func (o CsvReaderOpts) WithAllocator(mem memory.Allocator) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithAllocator(mem)),
	}
}

func (o CsvReaderOpts) WithChunk(n int) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithChunk(n)),
	}
}

func (o CsvReaderOpts) WithColumnTypes(types map[string]arrow.DataType) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithColumnTypes(types)),
	}
}

func (o CsvReaderOpts) WithComma(c rune) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithComma(c)),
	}
}

func (o CsvReaderOpts) WithComment(c rune) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithComment(c)),
	}
}

func (o CsvReaderOpts) WithHeader(useHeader bool) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithHeader(useHeader)),
	}
}

func (o CsvReaderOpts) WithIncludeColumns(cols []string) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithIncludeColumns(cols)),
	}
}

func (o CsvReaderOpts) WithLazyQuotes(useLazyQuotes bool) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithLazyQuotes(useLazyQuotes)),
	}
}

func (o CsvReaderOpts) WithNullReader(stringsCanBeNull bool, nullValues ...string) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithNullReader(stringsCanBeNull, nullValues...)),
	}
}

func (o CsvReaderOpts) WithStringsReplacer(replacer *strings.Replacer) CsvReaderOpts {
	return CsvReaderOpts{
		Opts: append(o.Opts, ac.WithStringsReplacer(replacer)),
	}
}

func (o CsvReaderOpts) ToReader(r io.Reader, schema *arrow.Schema) *CsvReader {
	return &CsvReader{
		reader: ac.NewReader(r, schema, o.Opts...),
		schema: schema,
		first:  nil,
		used:   false,
	}
}

func (o CsvReaderOpts) ToInferringReader(r io.Reader) *CsvReader {
	return &CsvReader{
		reader: ac.NewInferringReader(r, o.Opts...),
		schema: nil,
		first:  nil,
		used:   false,
	}
}
