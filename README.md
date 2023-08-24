# SingleStoreDB Go Arrow Driver
The SingleStoreDB Go Arrow driver facilitates the reading of data in Apache Arrow format from SingleStoreDB databases. Note that this is the alpha release of the driver, and there may be changes to the API, type conversion, and other internal implementations in the future.

## Installation
To add the SingleStoreDB Go Arrow Driver as a dependency to your Go module, use the following command:
```
go get github.com/singlestore-labs/singlestoredb-go-arrow-driver
```

## API

The `S2DBArrowReader` provides an API for reading Apache Arrow data from the SingleStore database. To create a new instance of `S2DBArrowReader`, use the `NewS2DBArrowReader` function. Here is an overview of the methods provided by `S2DBArrowReader`:
  * `GetNextArrowRecordBatch`. Retrieves a single `arrow.Record` from the database. It returns `nil` as the first part of the result tuple when there are no more rows to fetch. The returned `Record` must be released using the `Release()` method after use.
  * `Close`. Finalizes the reading of query results and releases all acquired resources.

## Configuration

The `NewS2DBArrowReader` function takes the `S2DBArrowReaderConfig` as a parameter. Here are the reader configurations and their explanations:
| Name               | Default               | Description  | 
| :------------------| :-------------------- | :----------- |
| Conn               | No default (required) | The `sql.DB` object used for communication with the database.
| Args               | nil (no arguments)    | Arguments for placeholder parameters in the query.
| RecordSize         | 10000                 | The maximum number of rows in the resulting records.
| ParallelReadConfig | nil (sequential read) | Additional configurations for parallel read. If this value is non-`nil`, parallel read is enabled.

The `S2DBParallelReadConfig` allows you to configure additional settings for parallel read. Here are the additional configurations that can be set:

| Name               | Default               | Description  | 
| :------------------| :-------------------- | :----------- |
| DatabaseName       | No default (required) | The name of the SingleStore database. This is required to determine the number of partitions for parallel reading.
| ChannelSize        | 10000                 | The size of the channel buffer. The channel stores references to Arrow Records while reading is in progress and transfers them to the main goroutine.

## Usage example

```go
db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
if err != nil {
    // Handle the error
}

arrowReader, err := s2db_arrow_driver.NewS2DBArrowReader(
    context.Background(), 
    s2db_arrow_driver.S2DBArrowReaderConfig{
	    Conn:  db,
	    Query: "SELECT * FROM t",
	    ParallelReadConfig: &s2db_arrow_driver.S2DBParallelReadConfig{
		    DatabaseName: "db",
	    },
    })
if err != nil {
    // Handle the error
}
defer arrowReader.Close()

for batch, err := arrowReader.GetNextArrowRecordBatch(); batch != nil; batch, err = arrowReader.GetNextArrowRecordBatch() {
	if err != nil {
        // Handle the error
	}
    defer batch.Release()

    // Process the batch
}
```

## Performance Considerations

For achieving maximum performance, consider using parallel read. The speed of parallel read depends on the size of the SingleStore cluster and the number of CPU cores on the machine where the code runs. It is recommended to use a machine with a number of CPU cores equal to the number of partitions in the SingleStore database, although this is not mandatory.

Additionally, performance is influenced by the data types in the SingleStore database. Performance tests conducted by the SingleStore team demonstrated that nullable data types are slower than non-nullable ones. Therefore, consider using non-nullable data types when appropriate.

## Data type mapping
The following table illustrates the type mapping between SingleStore data types and Arrow data types. Please note that this mapping is based on the alpha version of the driver and might change in the future.

| SingleStore Data Type | Arrow Data Type    | 
| :-------------------- | :----------------- |
| UNSIGNED TINYINT      | uint8
| UNSIGNED SMALLINT     | uint16
| UNSIGNED MEDIUMINT    | uint32
| UNSIGNED INT          | uint32
| UNSIGNED BIGINT       | uint64
| TINYINT               | boolean
| SMALLINT              | int16
| MEDIUMINT             | int32
| INT                   | int32
| BIGINT                | int64
| FLOAT                 | float32
| DOUBLE                | float64
| DECIMAL               | string
| YEAR                  | int16
| DATE                  | string
| TIME                  | string
| DATETIME              | string
| TIMESTAMP             | string
| CHAR                  | string
| VARCHAR               | string
| TINYTEXT              | string
| TEXT                  | string
| MEDIUMTEXT            | string
| LONGTEXT              | string
| JSON                  | string
| BIT                   | binary
| BINARY                | binary
| VARBINARY             | binary
| TINYBLOB              | binary
| BLOB                  | binary
| MEDIUMBLOB            | binary
| LONGBLOB              | binary
