# xl_commit_ts_truncate

## Location
src/include/access/commit_ts.h: 60 - 64

## Overview
The `xl_commit_ts_truncate` structure represents the WAL record format for logging commit timestamp data truncation operations during maintenance activities.

## Definition
```c
typedef struct xl_commit_ts_truncate
{
    int64       pageno;
    TransactionId oldestXid;
} xl_commit_ts_truncate;
```

## Detailed Description
This structure defines the format of WAL records used to log truncation operations in the commit timestamp subsystem. When PostgreSQL needs to reclaim space or perform maintenance on commit timestamp data, it creates a truncate record to ensure the operation can be properly replayed during recovery.

The structure contains information about which page is being truncated and what the oldest remaining transaction ID will be after the truncation operation. This ensures that during recovery, the system knows exactly which commit timestamp data should be preserved and which can be safely discarded.

## Parameters / Member Variables
- `pageno`: A 64-bit page number (int64) indicating which commit timestamp page is being truncated
- `oldestXid`: The oldest transaction ID (TransactionId) that should be preserved after the truncation operation

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (type definition)
  - int64 (type definition)
- Called from (representative examples):
  - [WriteTruncateXlogRec](../W/WriteTruncateXlogRec.md) function in src/backend/access/transam/commit_ts.c:1009
  - [commit_ts_redo](../c/commit_ts_redo.md) function for WAL replay in src/backend/access/transam/commit_ts.c:1049
  - [commit_ts_desc](../c/commit_ts_desc.md) function for WAL record description in src/backend/access/rmgrdesc/committsdesc.c:35
  - SizeOfCommitTsTruncate macro calculation

## Notes and Other Information
- Used in conjunction with the COMMIT_TS_TRUNCATE WAL record type (0x10)
- The `SizeOfCommitTsTruncate` macro calculates the size of this structure for WAL record operations
- Truncation operations are necessary for space management in the commit timestamp SLRU (Simple LRU) system
- This record type ensures that truncation operations are crash-safe and can be properly replayed during recovery
- The pageno field uses int64 to handle large page numbers as the system scales