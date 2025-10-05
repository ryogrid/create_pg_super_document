# changes_filename

## Location
[src/backend/replication/logical/worker.c:4204-4217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4204-L4217)

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
  - [stream_abort_internal](../s/stream_abort_internal.md)
  - [ensure_last_message](../e/ensure_last_message.md)  
  - [apply_spooled_messages](../a/apply_spooled_messages.md)
  - [stream_cleanup_files](../s/stream_cleanup_files.md)
  - [stream_open_file](../s/stream_open_file.md)

## Notes and Other Information
- The function assumes the output buffer has sufficient space (MAXPGPATH bytes)
- The ".changes" extension is hardcoded and indicates the file contains serialized logical replication changes
- This is a static inline function, meaning it is only visible within the worker.c compilation unit and is typically inlined at call sites for performance
- The filename format enables easy identification and cleanup of change files based on subscription and transaction boundaries

## Simplified Source

```c
static inline void changes_filename(char *path, Oid subid, TransactionId xid) {
    snprintf(path, MAXPGPATH, "%u-%u.changes", subid, xid);
}
```