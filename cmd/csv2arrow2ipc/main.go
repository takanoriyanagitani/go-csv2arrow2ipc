package main

import (
	"context"
	"errors"
	"flag"
	"log"

	ci "github.com/takanoriyanagitani/go-csv2arrow2ipc"
)

var errCommaSingleChar = errors.New("comma must be a single character")
var errCommentSingleChar = errors.New("comment must be a single character")

func setReaderOpts(
	comma, comment rune,
	noHeader bool,
	opts ci.CsvReaderOpts,
) ci.CsvReaderOpts {
	if comma != 0 {
		opts = opts.WithComma(comma)
	}
	if comment != 0 {
		opts = opts.WithComment(comment)
	}
	if noHeader {
		opts = opts.WithHeader(false)
	}
	return opts
}

func setWriterOpts(
	compressLZ4, compressZstd, noDictDelta bool,
	opts ci.IpcWriteOpts,
) ci.IpcWriteOpts {
	if compressLZ4 {
		opts = opts.WithLZ4()
	}
	if compressZstd {
		opts = opts.WithZstd()
	}
	if noDictDelta {
		opts = opts.WithDictionaryDeltas(false)
	}
	return opts
}

func onOpt(opts ci.CsvToIpcOpts) ci.CsvToIpcOpts {
	var comma, comment rune
	var noHeader bool
	var compressLZ4, compressZstd, noDictDelta bool

	// reader flags
	flag.Func("comma", "field delimiter", func(s string) error {
		if len(s) != 1 {
			return errCommaSingleChar
		}
		comma = []rune(s)[0]
		return nil
	})
	flag.Func("comment", "comment character", func(s string) error {
		if len(s) != 1 {
			return errCommentSingleChar
		}
		comment = []rune(s)[0]
		return nil
	})
	flag.BoolVar(&noHeader, "no-header", false, "disable header handling")

	// writer flags
	flag.BoolVar(&compressLZ4, "compress-lz4", false, "use lz4 compression")
	flag.BoolVar(&compressZstd, "compress-zstd", false, "use zstd compression")
	flag.BoolVar(&noDictDelta, "no-dict-delta", false, "disable dictionary deltas")

	flag.Parse()

	opts.Reader = setReaderOpts(comma, comment, noHeader, opts.Reader)
	opts.Writer = setWriterOpts(compressLZ4, compressZstd, noDictDelta, opts.Writer)

	return opts
}

func main() {
	opts := ci.NewCsvToIpcOpts()
	opts = onOpt(opts)

	ctx := context.Background()

	err := opts.ConvertStdInferSchema(ctx)

	if err != nil {
		log.Fatal(err)
	}
}
