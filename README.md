# SingleStoreDB Go Arrow Driver

**Attention**: The code in this repository is intended for experimental use only and is not fully tested, documented, or supported by SingleStore. Visit the [SingleStore Forums](https://www.singlestore.com/forum/) to ask questions about this repository.

The SingleStoreDB Go Arrow driver facilitates the reading of data in Apache Arrow format from SingleStoreDB databases. Note that this is the alpha release of the driver, and there may be changes to the API, type conversion, and other internal implementations in the future.

## Installation
Run the following command to add the SingleStoreDB Go Arrow driver as a dependency to your Go module:
```
go get github.com/singlestore-labs/singlestoredb-go-arrow-driver
```

MySQL driver dependency is required to use this driver:
```
go get github.com/go-sql-driver/mysql@v1.7.2-0.20230809113539-7cf548287682
```

Use the following code to import dependencies:
```
import (
    "database/sql"

    _ "github.com/go-sql-driver/mysql"
    s2db_arrow_driver "github.com/singlestore-labs/singlestoredb-go-arrow-driver"
)
```

## API

The `S2DBArrowReader` interface provides an API for reading Apache Arrow data from SingleStoreDB databases. To create a new instance of `S2DBArrowReader`, use the `NewS2DBArrowReader` function. `S2DBArrowReader` provides the following methods:
  * `GetNextArrowRecordBatch`: Retrieves a single `Record` object (`arrow.Record`) from the database. When there are no more records to fetch, it returns `nil` as the first part of the result tuple. You must release the returned `Record` using the `Release()` method after use.
  * `Close`: Finalizes the reading of query results and releases all the acquired resources.

## Configuration

The `NewS2DBArrowReader` function takes `S2DBArrowReaderConfig` as a parameter. Here are the supported reader configurations and their explanations:
| Name               | Default               | Description  | 
| :------------------| :-------------------- | :----------- |
| Conn               | No default (required) | The `sql.DB` object used to connect with a SingleStoreDB database.
| Args               | nil (no arguments)    | Arguments for placeholder parameters in the query.
| RecordSize         | 10000                 | The maximum number of rows in the resulting records.
| ParallelReadConfig | nil (sequential read) | Additional configurations for parallel read. If this value is non-`nil`, parallel read is enabled.
| UseClientConvesion | false                 | Indicates if the data should be converted to Arrow Record format on the client. It can be set to `true` for test purposes, performance optimizations take place only when it is `false`
| EnableQueryLogging | false                 | Controls whether the driver should generate debug logs. Debug logs are printed to the standard output.

The `S2DBParallelReadConfig` allows you to configure additional settings for parallel read. Here are the additional configurations that can be set:

| Name                 | Default               | Description  | 
| :------------------- | :-------------------- | :----------- |
| DatabaseName         | No default (required) | The name of the SingleStoreDB database. It is used to determine the number of partitions for parallel reading.
| ChannelSize          | 10000                 | The size of the channel buffer. The channel stores references to Arrow Records while reading is in progress and transfers them to the main `goroutine`.
| EnableDebugProfiling | false                 | Controls whether to profile the query. Profiling result is printed to the standart output.

> Note: 
Set `interpolateParams=true` parameter of the `sql.DB` in order to use parallel read.
If this parameter is not set - you will get the following error: `This command is not supported in the prepared statement protocol yet`

> Note:
Currently parallel read with `UseClientConvesion = false` is not supported. So if you set `ParallelReadConfig`, you must also set `UseClientConvesion` to `true`.

## Usage example

```go
dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?interpolateParams=true", "user", "password", "host", 3306, "database")
dsn += fmt.Sprintf("&connectionAttributes=%s:%s,%s:%s", "program_name", "CompanyName_AppName", "program_version", "1.2.3")
db, err := sql.Open("mysql", dsn)
if err != nil {
    // Handle the error
}

arrowReader, err := s2db_arrow_driver.NewS2DBArrowReader(
    context.Background(), 
    s2db_arrow_driver.S2DBArrowReaderConfig{
        Conn:  db,
        Query: "SELECT * FROM t WHERE a > ? AND a < ?",
            Args: []interface{}{1, 10},
        // uncomment lines below to use parallel read instead of Arrow conversion on Server
        // ParallelReadConfig: &s2db_arrow_driver.S2DBParallelReadConfig{
        //     DatabaseName: "db",
        // },
        // UseClientConvesion: true,
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

~~To achieve maximum performance, consider using parallel read. The performance of parallel read depends on the size of the SingleStore cluster and the number of CPU cores on the machine where the code runs. SingleStore recommends using a machine where the number of CPU cores is equal to the number of partitions in the SingleStoreDB database.~~ While the above holds true, parallel read will be supported in the next release.

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
