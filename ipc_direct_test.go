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

func TestIpcRoundtrip(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tmpfile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema(
			[]arrow.Field{
				{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
				{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			},
			nil,
		)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5}, nil)

		rec := b.NewRecord()
		defer rec.Release()

		w, err := ipc.NewFileWriter(tmpfile, ipc.WithSchema(schema))
		require.NoError(t, err)
		err = w.Write(rec)
		require.NoError(t, err)
		err = w.Close()
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
		require.Equal(t, int64(5), readRec.NumRows())
		require.Equal(t, int64(2), readRec.NumCols())
	})
}
