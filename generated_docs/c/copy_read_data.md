# copy_read_data

## Location
[src/backend/replication/logical/tablesync.c:744-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L744-L819)

## Overview
A data source callback function for PostgreSQL's COPY FROM operation that reads data from a remote logical replication connection and passes it to the local COPY process.

## Definition

```c
static int
copy_read_data(void *outbuf, int minread, int maxread)
```
## Detailed Description
The  function serves as the data source callback for COPY FROM operations during logical replication table synchronization. It acts as an intermediary between the remote connection and the local COPY process, reading data from the publisher via the WAL receiver connection and buffering it for the COPY operation.

The function implements a sophisticated buffering mechanism using a static  (StringInfo structure) to handle partial reads and ensure efficient data transfer. It handles several scenarios:

1. **Buffer Management**: Uses leftover data from previous reads when available
2. **Remote Data Retrieval**: Reads data from the publisher using  
3. **Asynchronous I/O**: Waits for data using latches and socket readiness when no data is immediately available
4. **Flow Control**: Respects minimum and maximum read requirements from the COPY process

The function continues reading until either the minimum read requirement is satisfied or no more data is available. It uses PostgreSQL's latch mechanism to efficiently wait for network data without busy-waiting.

## Parameters / Member Variables
- `*outbuf`: Pointer to the output buffer where data should be written for the COPY process
- `minread`: Minimum number of bytes that should be read if possible (for efficiency)
- `maxread`: Maximum number of bytes that can be read in this call (buffer size limit)
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

## Simplified Source

```c
static int copy_read_data(void *outbuf, int minread, int maxread)
{
    int bytesread = 0;
    int avail;

    // Use any leftover data from previous reads
    avail = copybuf->len - copybuf->cursor;
    if (avail) {
        if (avail > maxread)
            avail = maxread;
        memcpy(outbuf, &copybuf->data[copybuf->cursor], avail);
        copybuf->cursor += avail;
        maxread -= avail;
        bytesread += avail;
    }

    // Continue reading until minimum requirements met
    while (maxread > 0 && bytesread < minread) {
        pgsocket fd = PGINVALID_SOCKET;
        int len;
        char *buf = NULL;

        // Try to receive data from WAL receiver
        for (;;) {
            len = walrcv_receive(LogRepWorkerWalRcvConn, &buf, &fd);

            CHECK_FOR_INTERRUPTS();

            if (len == 0)
                break;  // No data available
            else if (len < 0)
                return bytesread;  // Error occurred
            else {
                // Process received data - copy what fits
                copybuf->data = buf;
                copybuf->len = len;
                copybuf->cursor = 0;

                avail = Min(copybuf->len, maxread);
                memcpy(outbuf, copybuf->data, avail);
                outbuf = (char *)outbuf + avail;
                copybuf->cursor += avail;
                maxread -= avail;
                bytesread += avail;
            }

            if (maxread <= 0 || bytesread >= minread)
                return bytesread;
        }

        // Wait for more data on socket
        WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE | WL_LATCH_SET |
                         WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         fd, 1000L, WAIT_EVENT_LOGICAL_SYNC_DATA);
        ResetLatch(MyLatch);
    }

    return bytesread;
}
```