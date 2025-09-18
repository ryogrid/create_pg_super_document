# SendCopyDone

## Location
src/backend/backup/basebackup_copy.c: 331 - 340

## Overview
SendCopyDone is a static function that sends a CopyDone message to signal the completion of a COPY operation during PostgreSQL base backup streaming.

## Definition
```c
static void SendCopyDone(void)
```

## Detailed Description
This function sends a CopyDone message as part of the PostgreSQL frontend/backend protocol to indicate that all data has been sent and the COPY operation is complete. It is used specifically in base backup operations to signal the end of data streaming. The function simply sends an empty message with the PqMsg_CopyDone message type.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pq_putemptymessage
  - PqMsg_CopyDone
- Called from (representative examples):
  - bbsink_copystream_end_backup

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- The function sends an empty message, as CopyDone messages carry no payload
- Used to properly terminate COPY protocol sessions during base backup operations
- Part of the cleanup process when ending backup streaming operations