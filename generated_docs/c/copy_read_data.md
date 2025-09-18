# copy_read_data

## Location
src/backend/replication/logical/tablesync.c: 744 - 819

## Overview
A data source callback function for PostgreSQL's COPY FROM operation that reads data from a remote logical replication connection and passes it to the local COPY process.

## Definition


## Detailed Description
The  function serves as the data source callback for COPY FROM operations during logical replication table synchronization. It acts as an intermediary between the remote connection and the local COPY process, reading data from the publisher via the WAL receiver connection and buffering it for the COPY operation.

The function implements a sophisticated buffering mechanism using a static  (StringInfo structure) to handle partial reads and ensure efficient data transfer. It handles several scenarios:

1. **Buffer Management**: Uses leftover data from previous reads when available
2. **Remote Data Retrieval**: Reads data from the publisher using  
3. **Asynchronous I/O**: Waits for data using latches and socket readiness when no data is immediately available
4. **Flow Control**: Respects minimum and maximum read requirements from the COPY process

The function continues reading until either the minimum read requirement is satisfied or no more data is available. It uses PostgreSQL's latch mechanism to efficiently wait for network data without busy-waiting.

## Parameters / Member Variables
- : Pointer to the output buffer where data should be written for the COPY process
- : Minimum number of bytes that should be read if possible (for efficiency)  
- : Maximum number of bytes that can be read in this call (buffer size limit)

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_receive (reads data from WAL receiver connection)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (waits for socket readability or latch events)
  - [ResetLatch](../R/ResetLatch.md) (resets the process latch after wakeup)
  - CHECK_FOR_INTERRUPTS (checks for query cancellation)
  - memcpy (copies data between buffers)
  - LogRepWorkerWalRcvConn (global WAL receiver connection)
  - MyLatch (process latch for event notification)
  - copybuf (static StringInfo buffer for data buffering)

- Called from (representative examples):
  - [copy_table](copy_table.md) (registers this as a callback for COPY FROM operations)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:744-819
- This is a static helper function used internally within the tablesync module
- The function implements a state machine that handles partial reads and network I/O efficiently
- Uses PostgreSQL's StringInfo structure () as a static buffer to maintain state between calls
- Returns the actual number of bytes read, which may be less than  if no more data is available
- Handles socket timeouts (1000ms) and various latch events (WL_LATCH_SET, WL_TIMEOUT, WL_EXIT_ON_PM_DEATH)
- The callback interface allows it to be used with PostgreSQL's standard COPY infrastructure
- Critical for performance during initial table synchronization as it manages the data flow between publisher and subscriber efficiently