package csv2ipc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
)

type CsvToIpcOpts struct {
	Reader CsvReaderOpts
	Writer IpcWriteOpts
}

func NewCsvToIpcOpts() CsvToIpcOpts {
	return CsvToIpcOpts{
		Reader: CsvReaderOpts{Opts: nil},
		Writer: IpcWriteOpts{Opts: nil},
	}
}

func (o CsvToIpcOpts) Convert(
	ctx context.Context,
	rdr io.Reader,
	wtr io.Writer,
	sch *arrow.Schema,
) error {
	var reader *CsvReader
	if sch != nil {
		reader = o.Reader.ToReader(rdr, sch)
	} else {
		reader = o.Reader.ToInferringReader(rdr)
		sch = reader.Schema() // infer schema
	}

	rows := reader.ToIter()
	writerOpts := o.Writer.WithSchema(sch)
	return writerOpts.WriteAll(ctx, rows, wtr)
}

func (o CsvToIpcOpts) ConvertInferSchema(
	ctx context.Context,
	rdr io.Reader,
	wtr io.Writer,
) error {
	return o.Convert(ctx, rdr, wtr, nil)
}

func (o CsvToIpcOpts) ConvertStdWithSchema(ctx context.Context, sch *arrow.Schema) error {
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	err := o.Convert(ctx, reader, writer, sch)
	flushErr := writer.Flush()

	return errors.Join(err, flushErr)
}

func (o CsvToIpcOpts) ConvertStdInferSchema(ctx context.Context) error {
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	err := o.ConvertInferSchema(ctx, reader, writer)
	flushErr := writer.Flush()

	return errors.Join(err, flushErr)
}
