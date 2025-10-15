package csv2ipc_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	csv2ipc "github.com/takanoriyanagitani/go-csv2arrow2ipc"
)

func BenchmarkConvertInfer(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		csvData := "a,b,c\n" + strings.Repeat("1,2,3\n", 1000)
		csv := strings.NewReader(csvData)
		opts := csv2ipc.NewCsvToIpcOpts()
		opts.Reader = opts.Reader.WithHeader(true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := csv.Seek(0, 0)
			if err != nil {
				b.Fatal(err)
			}
			err = opts.Convert(context.Background(), csv, io.Discard, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConvertWithSchema(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)
		csvData := "a,b,c\n" + strings.Repeat("1,2,3\n", 1000)
		csv := strings.NewReader(csvData)
		opts := csv2ipc.NewCsvToIpcOpts()
		opts.Reader = opts.Reader.WithHeader(true)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := csv.Seek(0, 0)
			if err != nil {
				b.Fatal(err)
			}
			err = opts.Convert(context.Background(), csv, io.Discard, schema)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConvertWithSchemaLargeCompressed(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			{Name: "c", Type: arrow.PrimitiveTypes.Int64},
		}
		schema := arrow.NewSchema(fields, nil)
		csvData := "a,b,c\n" + strings.Repeat("1,2,3\n", 100000)
		csv := strings.NewReader(csvData)
		opts := csv2ipc.NewCsvToIpcOpts()
		opts.Reader = opts.Reader.WithHeader(true)
		opts.Writer = opts.Writer.WithZstd()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := csv.Seek(0, 0)
			if err != nil {
				b.Fatal(err)
			}
			err = opts.Convert(context.Background(), csv, io.Discard, schema)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
