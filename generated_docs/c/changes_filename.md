# changes_filename

## Location
src/backend/replication/logical/worker.c: 4204 - 4217

## Overview
Formats a standardized filename for files containing serialized changes in PostgreSQL logical replication streaming.

## Definition
```c
static inline void changes_filename(char *path, Oid subid, TransactionId xid)
```

## Detailed Description
This inline utility function generates a consistent filename format for files that store serialized changes during logical replication streaming operations. The function creates a filename using the pattern `{subid}-{xid}.changes` where subid is the subscription ID and xid is the transaction ID. This standardized naming convention allows the system to uniquely identify and manage change files for different subscriptions and transactions.

## Parameters / Member Variables
- `path`: Output buffer to store the formatted filename (must be at least MAXPGPATH bytes)
- `subid`: Subscription ID (Oid) used as the first part of the filename
- `xid`: Transaction ID (TransactionId) used as the second part of the filename

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C library function)
- Called from (representative examples):
  - stream_abort_internal
  - ensure_last_message  
  - apply_spooled_messages
  - stream_cleanup_files
  - stream_open_file

## Notes and Other Information
- The function assumes the output buffer has sufficient space (MAXPGPATH bytes)
- The ".changes" extension is hardcoded and indicates the file contains serialized logical replication changes
- This is a static inline function, meaning it is only visible within the worker.c compilation unit and is typically inlined at call sites for performance
- The filename format enables easy identification and cleanup of change files based on subscription and transaction boundaries