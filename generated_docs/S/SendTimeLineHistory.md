# SendTimeLineHistory

## Location
[src/backend/replication/walsender.c:593-682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L593-L682)

## Overview
SendTimeLineHistory handles the TIMELINE_HISTORY replication command by reading and transmitting the complete timeline history file for a specified timeline to replication clients.

## Definition
```c
static void SendTimeLineHistory(TimeLineHistoryCmd *cmd)
```

## Detailed Description
SendTimeLineHistory is a static function that implements the TIMELINE_HISTORY replication protocol command. This command allows replication clients to retrieve the timeline history file for a specific timeline, which contains information about timeline switches and is crucial for understanding the branching history of WAL segments in PostgreSQL replication scenarios.

The function creates a result set with two columns: the filename of the timeline history file and its complete contents. It constructs the appropriate filename and file path based on the requested timeline ID, opens the file using PostgreSQL's transient file management system, and streams the entire file contents to the client. The file is read in chunks using aligned buffers and transmitted using the PostgreSQL protocol messaging system.

The function includes comprehensive error handling for file operations including opening, seeking, reading, and closing the file. It reports wait events during file I/O operations for monitoring purposes and ensures proper cleanup even in error conditions.

## Parameters / Member Variables
- `cmd`: Pointer to a TimeLineHistoryCmd structure containing the timeline ID for which the history file should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitBuiltinEntry](../T/TupleDescInitBuiltinEntry.md)
  - TLHistoryFileName
  - TLHistoryFilePath
  - pq_beginmessage
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - pq_sendbytes
  - OpenTransientFile
  - lseek
  - read
  - CloseTransientFile
  - [pq_endmessage](../p/pq_endmessage.md)
  - pgstat_report_wait_start
  - pgstat_report_wait_end
  - DestRemoteSimple
  - PqMsg_DataRow
  - PG_BINARY
  - PGAlignedBlock
  - ERRCODE_DATA_CORRUPTED

- Called from:
  - [exec_replication_command](../e/exec_replication_command.md) (when processing TIMELINE_HISTORY command)

## Notes and Other Information
- This is a static function only accessible within walsender.c
- Timeline history files contain information about timeline switches and are essential for replication clients to understand WAL timeline branching
- Uses PostgreSQL's transient file management for proper resource handling and cleanup
- Implements streaming file transfer by reading in chunks rather than loading the entire file into memory
- Includes comprehensive error handling for all file I/O operations with appropriate error codes
- Reports wait events during file reads for performance monitoring and troubleshooting
- The function sends data using PostgreSQL's protocol messaging system with proper message framing
- Timeline history files are typically small but the streaming approach ensures scalability
- Part of PostgreSQL's streaming replication protocol and used by standby servers to understand timeline history