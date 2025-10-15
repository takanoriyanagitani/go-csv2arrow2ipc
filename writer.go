package csv2ipc

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	ai "github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type IpcWriter struct{ *ai.FileWriter }

func (w IpcWriter) Close() error { return w.FileWriter.Close() }
func (w IpcWriter) Write(b arrow.RecordBatch) error {
	return w.FileWriter.Write(b)
}

func (w IpcWriter) WriteAll(
	ctx context.Context,
	rows iter.Seq2[arrow.RecordBatch, error],
) error {
	for batch, iterErr := range rows {
		if iterErr != nil {
			return iterErr
		}

		select {
		case <-ctx.Done():
			batch.Release()
			return ctx.Err()
		default:
		}
		err := w.Write(batch)
		batch.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

type IpcWriteOpts struct {
	Opts []ai.Option
}

func NewIpcWriteOpts() IpcWriteOpts {
	return IpcWriteOpts{Opts: nil}
}

func (o IpcWriteOpts) WithCompressConcurrency(n int) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithCompressConcurrency(n)),
	}
}

func (o IpcWriteOpts) WithSchema(schema *arrow.Schema) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithSchema(schema)),
	}
}

func (o IpcWriteOpts) WithDictionaryDeltas(enabled bool) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithDictionaryDeltas(enabled)),
	}
}

func (o IpcWriteOpts) WithAllocator(mem memory.Allocator) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithAllocator(mem)),
	}
}

func (o IpcWriteOpts) WithDelayReadSchema(v bool) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithDelayReadSchema(v)),
	}
}

func (o IpcWriteOpts) WithEnsureNativeEndian(v bool) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithEnsureNativeEndian(v)),
	}
}

func (o IpcWriteOpts) WithFooterOffset(offset int64) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithFooterOffset(offset)),
	}
}

func (o IpcWriteOpts) WithLZ4() IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithLZ4()),
	}
}

func (o IpcWriteOpts) WithMinSpaceSavings(savings float64) IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithMinSpaceSavings(savings)),
	}
}

func (o IpcWriteOpts) WithZstd() IpcWriteOpts {
	return IpcWriteOpts{
		Opts: append(o.Opts, ai.WithZstd()),
	}
}

func (o IpcWriteOpts) ToWriter(wtr io.Writer) (IpcWriter, error) {
	w, err := ai.NewFileWriter(wtr, o.Opts...)
	if err != nil {
		return IpcWriter{}, err
	}
	return IpcWriter{w}, nil
}

func (o IpcWriteOpts) WriteAll(
	ctx context.Context,
	rows iter.Seq2[arrow.RecordBatch, error],
	wtr io.Writer,
) error {
	// This assumes the schema is already in o.Opts
	writer, err := o.ToWriter(wtr)
	if err != nil {
		return err
	}

	writeErr := writer.WriteAll(ctx, rows)
	closeErr := writer.Close()
	return errors.Join(writeErr, closeErr)
}
