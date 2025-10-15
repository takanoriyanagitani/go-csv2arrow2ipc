package csv2ipc_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
	csv2ipc "github.com/takanoriyanagitani/go-csv2arrow2ipc"
)

func TestCsvReader(t *testing.T) {
	t.Parallel()

	t.Run("WithComma", func(t *testing.T) {
		t.Parallel()

		csv := strings.NewReader("a;b;c\n1;2;3")
		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)

		opts := csv2ipc.CsvReaderOpts{}.WithComma(';').WithHeader(true)
		reader := opts.ToReader(csv, schema)

		require.True(t, reader.Next())
		rec := reader.Record()
		require.Equal(t, int64(3), rec.NumCols())
		require.Equal(t, int64(1), rec.NumRows())

		require.False(t, reader.Next())
		require.NoError(t, reader.Err())
	})
}
