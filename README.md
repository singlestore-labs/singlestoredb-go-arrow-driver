# SingleStoreDB Go Arrow Driver
The SingleStoreDB Go Arrow driver facilitates the reading of data in Apache Arrow format from SingleStoreDB databases. Note that this is the alpha release of the driver, and there may be changes to the API, type conversion, and other internal implementations in the future.

## Installation
Run the following command to add the SingleStoreDB Go Arrow driver as a dependency to your Go module:
```
go get github.com/singlestore-labs/singlestoredb-go-arrow-driver
```

## API

The `S2DBArrowReader` provides an API for reading Apache Arrow data from SingleStoreDB databases. To create a new instance of `S2DBArrowReader`, use the `NewS2DBArrowReader` function. `S2DBArrowReader` provides the following methods:
  * `GetNextArrowRecordBatch`: Retrieves a single `Record` object (`arrow.Record`) from the database. When there are no more records to fetch, it returns `nil` as the first part of the result tuple. You must release the returned `Record` using the `Release()` method after use.
  * `Close`: Finalizes the reading of query results and releases all the acquired resources.

## Configuration

The `NewS2DBArrowReader` function takes `S2DBArrowReaderConfig` as a parameter. Here are the supported reader configurations and their explanations:
| Name               | Default               | Description  | 
| :------------------| :-------------------- | :----------- |
| Conn               | No default (required) | The `sql.DB` object used for communication with the database.
| Args               | nil (no arguments)    | Arguments for placeholder parameters in the query.
| RecordSize         | 10000                 | The maximum number of rows in the resulting records.
| ParallelReadConfig | nil (sequential read) | Additional configurations for parallel read. If this value is non-`nil`, parallel read is enabled.

The `S2DBParallelReadConfig` allows you to configure additional settings for parallel read. Here are the additional configurations that can be set:

| Name               | Default               | Description  | 
| :------------------| :-------------------- | :----------- |
| DatabaseName       | No default (required) | The name of the SingleStoreDB database. It is used to determine the number of partitions for parallel reading.
| ChannelSize        | 10000                 | The size of the channel buffer. The channel stores references to Arrow Records while reading is in progress and transfers them to the main `goroutine`.

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

To achieve maximum performance, consider using parallel read. The performance of parallel read depends on the size of the SingleStore cluster and the number of CPU cores on the machine where the code runs. SingleStore recommends using a machine where the number of CPU cores is equal to the number of partitions in the SingleStoreDB database.

Additionally, performance is influenced by the data types in the SingleStoreDB database. Performance tests conducted by the SingleStore team demonstrated that nullable data types are slower than non-nullable types. Therefore, consider using non-nullable data types when appropriate.

## Data type mapping
The following table maps the SingleStoreDB data types to the corresponding Arrow data types. Note that this mapping is based on the alpha version of the driver and it may change in the future.

| SingleStoreDB Data Type | Arrow Data Type    | 
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
