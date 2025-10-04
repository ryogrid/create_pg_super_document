# bbsink_copystream_begin_archive

## Location
[src/backend/backup/basebackup_copy.c:165-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L165-L182)

## Overview
Sends a CopyData protocol message to announce the beginning of a new archive in the backup stream.

## Definition
static void bbsink_copystream_begin_archive(bbsink *sink, const char *archive_name)

## Detailed Description
This function creates and sends a CopyData message that signals the start of a new archive within the backup stream. The message contains a type byte 'n' (for "New archive") followed by the archive name and the corresponding tablespace path. This allows the client to distinguish between different archives in the continuous COPY stream and properly organize the received backup data. The function retrieves the current tablespace information from the backup state to include the appropriate path information with the archive announcement.

## Parameters / Member Variables
- : Pointer to the base bbsink structure containing backup state information
- : Name of the archive being started (typically a tar filename)

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendstring](../p/pq_sendstring.md) (called twice)
  - [pq_endmessage](../p/pq_endmessage.md)
  - PqMsg_CopyData
  - [tablespaceinfo](../t/tablespaceinfo.md)
  - [bbsink_state](bbsink_state.md)
- Called from (representative examples):
  - Referenced by bbsink_copystream_ops structure as the begin_archive handler

## Notes and Other Information
- Sends a 'n' type byte to identify this as a new archive message in the protocol stream
- Includes both the archive name and tablespace path in the message payload
- Uses an empty string for the tablespace path if ti->path is NULL (typically for the main tablespace)
- This message format allows clients to reconstruct the proper directory structure for the backup
- The function accesses the current tablespace using state->tablespace_num as an index into the tablespaces list

## Simplified Source

```c
static void
bbsink_copystream_begin_archive(bbsink *sink, const char *archive_name)
{
    bbsink_state *state = sink->bbs_state;
    tablespaceinfo *ti;
    StringInfoData buf;

    // Get current tablespace info from the backup state
    ti = list_nth(state->tablespaces, state->tablespace_num);

    // Send CopyData message announcing new archive
    pq_beginmessage(&buf, PqMsg_CopyData);
    pq_sendbyte(&buf, 'n');  // 'n' = New archive
    pq_sendstring(&buf, archive_name);
    pq_sendstring(&buf, ti->path == NULL ? "" : ti->path);  // tablespace path
    pq_endmessage(&buf);
}
```