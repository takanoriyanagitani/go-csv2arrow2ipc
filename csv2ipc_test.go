package csv2ipc_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/require"
	csv2ipc "github.com/takanoriyanagitani/go-csv2arrow2ipc"
)

func TestConvertInferSchema(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tmpfile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		csv := strings.NewReader("a,b,c\n1,2,3\n4,5,6")

		opts := csv2ipc.NewCsvToIpcOpts()
		opts.Reader = opts.Reader.WithChunk(-1)
		err = opts.ConvertInferSchema(context.Background(), csv, tmpfile)
		require.NoError(t, err)

		f, err := os.Open(tmpfile.Name())
		require.NoError(t, err)
		defer f.Close()

		r, err := ipc.NewFileReader(f)
		require.NoError(t, err)
		defer r.Close()

		require.Equal(t, 1, r.NumRecords())
		rec, err := r.Record(0)
		require.NoError(t, err)
		require.Equal(t, int64(3), rec.NumCols())
		require.Equal(t, int64(2), rec.NumRows())
	})
}

func TestConvert(t *testing.T) {
	t.Parallel()

	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		tmpfile, err := os.CreateTemp("", "example")
		require.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		csv := strings.NewReader("1,2,3\n4,5,6")

		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)

		opts := csv2ipc.NewCsvToIpcOpts()
		opts.Reader = opts.Reader.WithChunk(-1)
		err = opts.Convert(context.Background(), csv, tmpfile, schema)
		require.NoError(t, err)

		f, err := os.Open(tmpfile.Name())
		require.NoError(t, err)
		defer f.Close()

		r, err := ipc.NewFileReader(f)
		require.NoError(t, err)
		defer r.Close()

		require.Equal(t, 1, r.NumRecords())
		rec, err := r.Record(0)
		require.NoError(t, err)
		require.Equal(t, int64(3), rec.NumCols())
		require.Equal(t, int64(2), rec.NumRows())
	})
}
