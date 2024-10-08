# crypto-data-service-loader
## About program
A standalone multithreaded service that handles uploading ticker data to **Clickhouse**. Data from Clickhouse is used in **Grafana** for analytics.  
All flows are using *lightweight workflow engine* - [Flower](https://github.com/ja-css/flower).

The file containing ticker data has the following format name:
ticker_name_date. For example:
```AVA-USDT_PST_2024-03-14```  
_Within the file itself, the data is stored in `CSV` format._  

All data is obtained using **KucoinAPI from the Kucoin exchange.**  
According to the API, the data contains the following information:
+ ticketName, 
+ sequence, 
+ price, 
+ size, 
+ bestAsk, 
+ bestAskSize, 
+ bestBid, 
+ bestBidSize, 
+ transactionTime

## Flows Description

The service consists of 4 flows working **independently and concurrently**. Flow diagram is presented [at the end of the readme](#flows-diagram).

Each flow has a parameter `WORK_CYCLE_TIME_HOURS/SEC`, which regulates the flow cycle time. **[For example](/MainService/src/main/resources/application.origin.yaml#L29).**  
`Jackson` is used to load application properties in `.yaml` format.

### Save new files to database flow

[Saving new files flow](#1-save-new-files-to-database-flow) detects new files provided by the service using **KucoinAPI**. When a new file appears on disk after a certain time this file will be placed in `filesBuffer` with the help of `WatcherService` and then information about it will be loaded into the database.  
The table with information about the file contains the following columns: 
+ filename, 
+ create_date, 
+ status.

Thus, the new file will be loaded with the `DISCOVERED` status.

### Proceed files status flow

[Proceed files flow](#2-proceed-files-status-flow) manages the statuses of files, namely, it puts them into `DOWNLOADING` or `READY_FOR_PROCESSING` statuses *according to flow logic (for more details - [link](/MainService/src/main/java/com/crypto/service/flow/ProceedFilesStatusFlow.java))*.   
**Depending on the file status, it will be processed by the third flow.**

### Upload ticker files status and data flow

[Uploading tickers data flow](#3-upload-ticker-files-status-and-data-flow) inserts data from ticker files into the database table `tickers_data`, if the file status is `READY_FOR_PROCESSING` - it means that the information about this ticker has been completely loaded and it will not be changed anymore.    

#### Achieving best performance for data upload operations
To achieve the best throughput we maintain 2 separate thread pools (`executor services` with 32 threads by default).  
The entire set of files is divided into equal bundles of files of [a certain size](/MainService/src/main/resources/application.origin.yaml#L16) so that **each thread can process its part of the files.**   
One half of these threads compresses the data into GZIP format and sends it to a special [PipedInputStream](https://docs.oracle.com/javase/8/docs/api/java/io/PipedInputStream.html), where the other half of the thread is waiting for it on [PipedOutputStream](https://docs.oracle.com/javase/8/docs/api/java/io/PipedOutputStream.html) and then loading the stream of **compressed files** directly into Clickhouse.  
At the same time, the status of the file in the database changes to `IN_PROGRESS`. If an exception is caught, the file status will be set to `ERROR`.  
Thanks to this joint work of the threads, we managed to achieve an upload speed of **300k - 500k rows/sec** on the Clickhouse server, which was hosted in ClickhouseCloud by Google in Netherlands. The same speed can be achieved using the Clickhouse CLI, but **not with** `JDBC or R2DBC Java clients`.

### Cleanup uploaded files flow

[Cleaning flow](#4-cleanup-uploaded-files-flow) deals with clearing files on disk in case they are successfully loaded into the database.  
According to [flow logic](/MainService/src/main/java/com/crypto/service/flow/CleanupUploadedFilesFlow.java), if the status of a file is `FINISHED`, it will be deleted from the disk.  
At the same time, in case of database errors or loading errors, a backup of the file is still stored on disk for a day.  
If the file processing status is `ERROR`, the file will not be deleted from disk, as it will require further reloading into the database.

### Logging

Logging is done with `SLF4J + Log4j2`.  
Log messages are also sent to the Clickhouse database using [a custom-written ClickhouseAppender](https://github.com/emelyanovkr/ClickHouseAppender/blob/main/src/main/java/com/clickhouse/appender/ClickHouseAppender.java), a special log message handler that uploads them to the database.  
`ClickhouseAppender` can be configured with [this file](/MainService/src/main/resources/log4j2.origin.xml).  
The log message in the database contains the message itself and its timestamp. The message is stored in `JSON` format.

### Testing

[Tests](/MainService/src/test/java/com/crypto/service/) are written using `JUnit 5 and Mockito`.  
The coverage of methods and classes in flows package reaches **79%**.

## Flows Diagram
### 1. Save new files to database flow
```mermaid
flowchart
BEGIN([BEGIN])
RETRIEVE_FILE_NAMES_LIST_ON_START[[RETRIEVE_FILE_NAMES_LIST_ON_START]]
BEGIN([BEGIN]) --> RETRIEVE_FILE_NAMES_LIST_ON_START[[RETRIEVE_FILE_NAMES_LIST_ON_START]]
RETRIEVE_FILE_NAMES_LIST_ON_START --> INIT_DIRECTORY_WATCHER_SERVICE[[INIT_DIRECTORY_WATCHER_SERVICE]]
TRY_TO_FLUSH_BUFFER[[TRY_TO_FLUSH_BUFFER]]
TRY_TO_FLUSH_BUFFER --> POST_FLUSH[[POST_FLUSH]]
GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER[[GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER]]
GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER --> TRY_TO_FLUSH_BUFFER[[TRY_TO_FLUSH_BUFFER]]
INIT_DIRECTORY_WATCHER_SERVICE[[INIT_DIRECTORY_WATCHER_SERVICE]]
INIT_DIRECTORY_WATCHER_SERVICE --> GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER[[GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER]]
POST_FLUSH[[POST_FLUSH]]
POST_FLUSH --> INIT_DIRECTORY_WATCHER_SERVICE[[INIT_DIRECTORY_WATCHER_SERVICE]]
POST_FLUSH --> GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER[[GET_DIRECTORY_WATCHER_EVENTS_AND_ADD_TO_BUFFER]]
```

### 2. Proceed files status flow
```mermaid
flowchart
BEGIN([BEGIN])
PROCEED_FILES_STATUS[[PROCEED_FILES_STATUS]]
PROCEED_FILES_STATUS --> RETRIEVE_TICKER_FILES_INFO[[RETRIEVE_TICKER_FILES_INFO]]
RETRIEVE_TICKER_FILES_INFO[[RETRIEVE_TICKER_FILES_INFO]]
BEGIN([BEGIN]) --> RETRIEVE_TICKER_FILES_INFO[[RETRIEVE_TICKER_FILES_INFO]]
RETRIEVE_TICKER_FILES_INFO --> PROCEED_FILES_STATUS[[PROCEED_FILES_STATUS]]
```

### 3. Upload ticker files status and data flow
```mermaid
flowchart
BEGIN([BEGIN])
RETRIEVE_PREPARED_FILES[[RETRIEVE_PREPARED_FILES]]
BEGIN([BEGIN]) --> RETRIEVE_PREPARED_FILES[[RETRIEVE_PREPARED_FILES]]
RETRIEVE_PREPARED_FILES --> FILL_PATHS_LIST[[FILL_PATHS_LIST]]
FILL_PATHS_LIST[[FILL_PATHS_LIST]]
FILL_PATHS_LIST --> UPLOAD_TICKERS_FILES_DATA[[UPLOAD_TICKERS_FILES_DATA]]
UPLOAD_TICKERS_FILES_DATA[[UPLOAD_TICKERS_FILES_DATA]]
UPLOAD_TICKERS_FILES_DATA --> RETRIEVE_PREPARED_FILES[[RETRIEVE_PREPARED_FILES]]
```

### 4. Cleanup uploaded files flow
```mermaid
flowchart
BEGIN([BEGIN])
CLEANUP_FILES[[CLEANUP_FILES]]
CLEANUP_FILES --> PREPARE_TO_CLEAN_FILES[[PREPARE_TO_CLEAN_FILES]]
PREPARE_TO_CLEAN_FILES[[PREPARE_TO_CLEAN_FILES]]
BEGIN([BEGIN]) --> PREPARE_TO_CLEAN_FILES[[PREPARE_TO_CLEAN_FILES]]
PREPARE_TO_CLEAN_FILES --> PREPARE_TO_CLEAN_FILES[[PREPARE_TO_CLEAN_FILES]]
PREPARE_TO_CLEAN_FILES --> CLEANUP_FILES[[CLEANUP_FILES]]
```
