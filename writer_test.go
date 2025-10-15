package csv2ipc_test

import (
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestIpcWriter(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		tmpfile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)

		writer, err := ipc.NewFileWriter(tmpfile, ipc.WithSchema(schema))
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		f, err := os.Open(tmpfile.Name())
		require.NoError(t, err)
		defer f.Close()

		r, err := ipc.NewFileReader(f)
		require.NoError(t, err)
		defer r.Close()

		require.Equal(t, 0, r.NumRecords())
	})

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tmpfile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)

		mem := memory.NewGoAllocator()
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		b.Field(0).(*array.Int64Builder).Append(1)
		rec := b.NewRecord()
		defer rec.Release()

		writer, err := ipc.NewFileWriter(tmpfile, ipc.WithSchema(schema))
		require.NoError(t, err)
		err = writer.Write(rec)
		require.NoError(t, err)
		err = writer.Close()
		require.NoError(t, err)

		f, err := os.Open(tmpfile.Name())
		require.NoError(t, err)
		defer f.Close()

		r, err := ipc.NewFileReader(f)
		require.NoError(t, err)
		defer r.Close()

		require.Equal(t, 1, r.NumRecords())
		readRec, err := r.Record(0)
		require.NoError(t, err)
		require.Equal(t, int64(1), readRec.NumRows())
		require.Equal(t, int64(1), readRec.NumCols())
	})
}
