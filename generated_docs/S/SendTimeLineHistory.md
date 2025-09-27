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
  - [TLHistoryFileName](../T/TLHistoryFileName.md)
  - [TLHistoryFilePath](../T/TLHistoryFilePath.md)
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendbytes](../p/pq_sendbytes.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - lseek
  - read
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
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

## Simplified Source

```c
// Simplified version of SendTimeLineHistory
static void SendTimeLineHistory(TimeLineHistoryCmd *cmd) {
    DestReceiver *dest;
    TupleDesc tupdesc;
    StringInfoData buf;
    char histfname[MAXFNAMELEN];
    char path[MAXPGPATH];
    int fd;
    off_t histfilelen;
    off_t bytesleft;

    // Step 1: Set up result destination for remote client
    dest = CreateDestReceiver(DestRemoteSimple);

    // Step 2: Create tuple descriptor for 2-column result set (filename, content)
    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitBuiltinEntry(tupdesc, 1, "filename", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, "content", TEXTOID, -1, 0);

    // Step 3: Generate timeline history filename and path
    TLHistoryFileName(histfname, cmd->timeline);
    TLHistoryFilePath(path, cmd->timeline);

    // Step 4: Send result set header to client
    dest->rStartup(dest, CMD_SELECT, tupdesc);

    // Step 5: Begin data row message with filename column
    pq_beginmessage(&buf, PqMsg_DataRow);
    pq_sendint16(&buf, 2);  // 2 columns
    pq_sendint32(&buf, strlen(histfname));
    pq_sendbytes(&buf, histfname, strlen(histfname));

    // Step 6: Open timeline history file
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not open file \"%s\": %m", path)));
    }

    // Step 7: Get file size and reset to beginning
    histfilelen = lseek(fd, 0, SEEK_END);
    if (histfilelen < 0 || lseek(fd, 0, SEEK_SET) != 0) {
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not seek in file \"%s\": %m", path)));
    }

    // Step 8: Send file size and stream file contents in chunks
    pq_sendint32(&buf, histfilelen);
    bytesleft = histfilelen;

    while (bytesleft > 0) {
        PGAlignedBlock rbuf;
        int nread;

        // Read chunk from file with wait event reporting
        pgstat_report_wait_start(WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ);
        nread = read(fd, rbuf.data, sizeof(rbuf));
        pgstat_report_wait_end();

        // Basic error checking for read operation
        if (nread <= 0) {
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not read file \"%s\": %m", path)));
        }

        // Send chunk to client and update remaining bytes
        pq_sendbytes(&buf, rbuf.data, nread);
        bytesleft -= nread;
    }

    // Step 9: Clean up file and complete message
    CloseTransientFile(fd);
    pq_endmessage(&buf);
}
```

Key simplifications made:
- Consolidated error handling for file operations
- Combined similar error reporting into single blocks
- Removed detailed error differentiation between read failures
- Simplified variable declarations and combined related operations
- Added step-by-step comments to clarify the main execution flow
- Focused on the core algorithm: setup → open file → stream contents → cleanup
- Maintained all essential functionality while improving readability