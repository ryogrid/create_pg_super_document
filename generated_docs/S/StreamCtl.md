# StreamCtl

## Location
[src/bin/pg_basebackup/receivelog.h:29-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/receivelog.h#L29-L49)

## Overview
StreamCtl is a control structure that encapsulates all global parameters needed for receiving WAL (Write-Ahead Log) stream data during PostgreSQL replication operations.

## Definition


## Detailed Description
StreamCtl serves as the central configuration structure for WAL streaming operations in PostgreSQL's base backup and WAL receiving utilities (pg_basebackup, pg_receivewal). This structure contains all necessary parameters to control how WAL data is received, processed, and written to disk during replication streaming. It provides fine-grained control over synchronization behavior, file handling, network communication, and streaming lifecycle management.

The structure is designed to be passed to the ReceiveXlogStream() function and other related WAL processing functions to maintain consistent streaming parameters across the entire operation.

## Parameters / Member Variables
- : XLogRecPtr indicating the WAL position from which to start streaming
- : TimeLineID specifying which timeline to stream data from
- : System identifier string used to validate that the source server matches expectations
- : Interval (in milliseconds) for sending status messages to the primary server
- : Boolean flag controlling whether WAL data should be flushed immediately upon write
- : Boolean flag indicating whether completed WAL segments should be marked as archived
- : Boolean flag controlling whether data should be flushed to disk to ensure consistency
- : Callback function pointer that returns true when streaming should be stopped
- : Socket descriptor to monitor for input that might trigger a stream stop check
- : Pointer to WalWriteMethod structure defining how WAL data should be written
- : String suffix appended to partially received WAL files
- : Name of the replication slot to use, or NULL if not using slots

## Dependencies
- Functions called/Symbols referenced:
  - pgsocket
  - [WalWriteMethod](../W/WalWriteMethod.md)
- Called from (representative examples):
  - [LogStreamerMain](../L/LogStreamerMain.md) (src/bin/pg_basebackup/pg_basebackup.c:547)
  - [StreamLog](StreamLog.md) (src/bin/pg_basebackup/pg_receivewal.c:504)
  - [ReceiveXlogStream](../R/ReceiveXlogStream.md) (src/bin/pg_basebackup/receivelog.c:453)
  - [HandleCopyStream](../H/HandleCopyStream.md) (src/bin/pg_basebackup/receivelog.c:745)
  - [ProcessXLogDataMsg](../P/ProcessXLogDataMsg.md) (src/bin/pg_basebackup/receivelog.c:1040)

## Notes and Other Information
- This structure is primarily used in PostgreSQL's client-side replication utilities
- The structure provides a clean abstraction for managing complex WAL streaming operations
- Located in src/bin/pg_basebackup/receivelog.h (lines 29-49)
- Essential for maintaining state consistency during long-running WAL streaming operations
- The stream_stop callback mechanism allows for graceful interruption of streaming operations
- Socket monitoring capability enables responsive handling of external stop signals