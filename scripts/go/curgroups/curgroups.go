package curgroups

import (
	"fmt"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
)

type MatchAny struct {
	All map[string]string `toml:"all"`
}

type CGMatch struct {
	Any []MatchAny `toml:"any"`
}

type CurGroup struct {
	Name        string  `toml:"name"`
	PathSuffix  string  `toml:"pathSuffix"`
	TablePrefix string  `toml:"tablePrefix"`
	Match       CGMatch `toml:"match"`
	f           ParquetFile.ParquetFile
	localFile   string
	ph          *ParquetWriter.CSVWriter
	cols        map[string]int
	Writer      chan []*string
	Block       chan bool
}

func (cg *CurGroup) CreateParquetWriter(localFile string, cols []string, colsPos map[string]int, concurrency int) error {
	var err error
	cg.f, err = ParquetFile.NewLocalFileWriter(localFile)
	if err != nil {
		return fmt.Errorf("failed to create parquet file %s, error: %s", localFile, err.Error())
	}

	cg.cols = colsPos
	cg.localFile = localFile

	// init Parquet writer
	cg.ph, err = ParquetWriter.NewCSVWriter(cols, cg.f, int64(concurrency))
	if err != nil {
		return err
	}

	// init channels
	cg.Writer = make(chan []*string, 100)
	cg.Block = make(chan bool)

	// start writer
	go cg.recordWriter()
	return nil
}

func (cg *CurGroup) GetUploadData() (string, string) {
	return cg.localFile, cg.PathSuffix
}

func (cg *CurGroup) CloseParquetWriter() {
	close(cg.Writer)
	<-cg.Block
}

func (cg *CurGroup) recordWriter() {
	flush := 1
	for m := range cg.Writer {
		if flush%5000 == 0 {
			cg.ph.Flush(true)
			flush = 1
		}

		var match bool
		for _, any := range cg.Match.Any {
			match = true
			for col, value := range any.All {
				if *m[cg.cols[col]] != value {
					match = false
					break
				}
			}
			if match {
				break
			}
		}
		if match {
			cg.ph.WriteString(m)
			flush++
		}
	}

	if flush > 1 {
		cg.ph.Flush(true)
	}
	cg.ph.WriteStop()
	cg.f.Close()

	cg.Block <- true
}
