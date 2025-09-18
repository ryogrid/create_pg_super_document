# LogStreamerMain

## Location
[src/bin/pg_basebackup/pg_basebackup.c:545-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L545-L615)

## Overview
The main function for the background WAL streaming process during a PostgreSQL base backup, responsible for setting up and executing WAL streaming operations.

## Definition
```c
static int LogStreamerMain(logstreamer_param *param)
```

## Detailed Description
`LogStreamerMain` serves as the entry point for the background WAL (Write-Ahead Log) streaming process during a base backup operation. It configures a `StreamCtl` structure with the appropriate parameters for streaming, including start position, timeline, and connection details. The function handles platform-specific differences between Unix and Windows implementations, particularly in signal handling and process communication. It creates the appropriate WAL method (either directory-based or tar-based) depending on the backup format, initiates the actual streaming via `ReceiveXlogStream`, and performs proper cleanup of resources upon completion or failure.

## Parameters / Member Variables  
- `param`: Pointer to logstreamer_param structure containing:
  - `bgconn`: Background database connection for streaming
  - `startptr`: XLogRecPtr indicating where to start streaming from
  - `xlog`: Directory or tar file path for storing WAL files
  - `sysidentifier`: System identifier string for validation
  - `timeline`: Timeline ID for the WAL stream
  - `wal_compress_algorithm`: Algorithm to use for WAL compression
  - `wal_compress_level`: Compression level setting

## Dependencies
- Functions called/Symbols referenced:
  - [reached_end_position](../r/reached_end_position.md) (callback function for stopping condition)
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (creates directory-based WAL storage method)
  - [CreateWalTarMethod](../C/CreateWalTarMethod.md) (creates tar-based WAL storage method)
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md) (performs the actual WAL streaming)
  - [PQfinish](../P/PQfinish.md) (closes database connection)
  - pg_log_error (error logging function)
- Global variables accessed:
  - in_log_streamer (flag indicating running in log streamer process)
  - bgpipe[0] (background pipe for communication on Unix)
  - standby_message_timeout, format, replication_slot
  - bgchild_exited (Windows-specific exit flag)
- Called from (representative examples):
  - [StartLogStreamer](../S/StartLogStreamer.md) function in pg_basebackup.c at lines 725 and 735

## Notes and Other Information
- Returns 0 on successful completion, 1 on error
- Sets in_log_streamer flag to indicate the process is running as a WAL streamer
- Uses different stop socket handling between Unix (pipe-based) and Windows (invalid socket)
- Configures streaming with fsync disabled since pg_basebackup performs final fsync
- Handles both plain format (directory method) and tar format (tar method) for WAL storage  
- Includes platform-specific error signaling for the parent process on failure
- Critical component of pg_basebackup's parallel streaming architecture