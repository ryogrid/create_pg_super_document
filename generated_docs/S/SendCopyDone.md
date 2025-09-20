# SendCopyDone

## Location
[src/backend/backup/basebackup_copy.c:331-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L331-L340)

## Overview
SendCopyDone is a static function that sends a CopyDone message to signal the completion of a COPY operation during PostgreSQL base backup streaming.

## Definition
```c
static void SendCopyDone(void)
```

## Detailed Description
This function sends a CopyDone message as part of the PostgreSQL frontend/backend protocol to indicate that all data has been sent and the COPY operation is complete. It is used specifically in base backup operations to signal the end of data streaming. The function simply sends an empty message with the PqMsg_CopyDone message type.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pq_putemptymessage](../p/pq_putemptymessage.md)
  - PqMsg_CopyDone
- Called from (representative examples):
  - [bbsink_copystream_end_backup](../b/bbsink_copystream_end_backup.md)

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- The function sends an empty message, as CopyDone messages carry no payload
- Used to properly terminate COPY protocol sessions during base backup operations
- Part of the cleanup process when ending backup streaming operations