package s2db_arrow_driver

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"golang.org/x/sync/errgroup"
)

type S2DBArrowReaderParallelImpl struct {
	conn            S2SqlDbWrapper
	databaseName    string
	channelSize     int64
	resultTableConn *sql.Conn
	resultTableName string
	ch              chan arrow.Record
	errorGroup      *errgroup.Group
	ctx             context.Context
}

func getPartitionsCount(ctx context.Context, conn S2SqlDbWrapper, database string) (int32, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = '%s'", database))
	if err != nil {
		return 0, err
	}

	if !rows.Next() {
		return 0, fmt.Errorf("database '%s' doesn't exist", database)
	}

	partitions := int32(1)
	err = rows.Scan(&partitions)
	if err != nil {
		return 0, err
	}
	return partitions, nil
}

func generateTableName(query string) string {
	return "goArrowResultTable_" + fmt.Sprintf("%x", md5.Sum([]byte(query))) + "_" + strconv.Itoa(rand.Intn(4294967295))
}

func NewS2DBArrowReaderParallelImpl(ctx context.Context, conf S2DBArrowReaderConfig) (S2DBArrowReader, error) {
	partitions, err := getPartitionsCount(ctx, conf.Conn, conf.ParallelReadConfig.DatabaseName)
	if err != nil {
		return nil, err
	}

	resultTableConn, err := conf.Conn.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			resultTableConn.Close()
		}
	}()

	resultTableName := generateTableName(conf.Query)
	createResultTableQuery := fmt.Sprintf("CREATE RESULT TABLE `%s` AS SELECT * FROM (%s)", resultTableName, conf.Query)
	if _, err = resultTableConn.ExecContext(ctx, createResultTableQuery, conf.Args...); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			resultTableConn.ExecContext(ctx,
				fmt.Sprintf("DROP RESULT TABLE `%s`", resultTableName))
		}
	}()

	ch := make(chan arrow.Record, conf.ParallelReadConfig.ChannelSize)
	finishedPartitions := int32(0)
	errorGroup := new(errgroup.Group)
	for i := int32(0); i < partitions; i++ {
		partition := i
		errorGroup.Go(
			func() error {
				defer func() {
					atomic.AddInt32(&finishedPartitions, 1)
					if atomic.LoadInt32(&finishedPartitions) == partitions {
						close(ch)
					}
				}()

				arrowReader, err := NewS2DBArrowReader(ctx, S2DBArrowReaderConfig{
					Conn:       conf.Conn,
					Query:      fmt.Sprintf("SELECT * FROM ::`%s` WHERE partition_id() = %d", resultTableName, partition),
					RecordSize: conf.RecordSize,
				})
				if err != nil {
					return err
				}
				defer arrowReader.Close()

				for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
					if err != nil {
						return err
					}

					ch <- batch
				}

				return nil
			},
		)
	}

	return &S2DBArrowReaderParallelImpl{
		conn:            conf.Conn,
		databaseName:    conf.ParallelReadConfig.DatabaseName,
		channelSize:     conf.ParallelReadConfig.ChannelSize,
		resultTableConn: resultTableConn,
		resultTableName: resultTableName,
		ch:              ch,
		errorGroup:      errorGroup,
		ctx:             ctx,
	}, nil
}

func (s2db *S2DBArrowReaderParallelImpl) GetNextArrowRecordBatch() (arrow.Record, error) {
	res := <-s2db.ch
	if res == nil {
		return nil, s2db.errorGroup.Wait()
	}

	return res, nil
}

func (s2db *S2DBArrowReaderParallelImpl) Close() error {
	if s2db.resultTableConn != nil {
		s2db.resultTableConn.ExecContext(s2db.ctx,
			fmt.Sprintf("DROP RESULT TABLE `%s`", s2db.resultTableName))
		return s2db.resultTableConn.Close()
	}

	return nil
}
