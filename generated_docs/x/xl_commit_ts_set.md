# xl_commit_ts_set

## Location
[src/include/access/commit_ts.h:49-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/commit_ts.h#L49-L55)

## Overview
The `xl_commit_ts_set` structure represents the WAL (Write-Ahead Log) record format for storing commit timestamp information that needs to be logged for recovery purposes.

## Definition
```c
typedef struct xl_commit_ts_set
{
    TimestampTz timestamp;
    RepOriginId nodeid;
    TransactionId mainxid;
    /* subxact Xids follow */
} xl_commit_ts_set;
```

## Detailed Description
This structure defines the format of WAL records used by the commit timestamp subsystem to log commit timestamp data during transaction commit. The commit timestamp feature allows PostgreSQL to track when each transaction was committed, which is useful for various applications including logical replication conflict resolution.

The structure serves as the header for a WAL record that contains the timestamp and replication origin information for a transaction. Following the header structure, additional transaction IDs for any sub-transactions may be appended to the record.

## Parameters / Member Variables
- `timestamp`: The commit timestamp (TimestampTz) when the transaction was committed
- `nodeid`: The replication origin identifier (RepOriginId) indicating which replication node the transaction originated from
- `mainxid`: The transaction ID (TransactionId) of the main transaction being committed
- *subxact Xids*: Variable-length array of sub-transaction IDs that follow the structure in memory

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (type definition)
  - TimestampTz (type definition)  
  - TransactionId (type definition)
- Called from (representative examples):
  - SizeOfCommitTsSet macro calculation
  - WAL logging routines during transaction commit

## Notes and Other Information
- The structure has a variable length due to the sub-transaction IDs that may follow
- The `SizeOfCommitTsSet` macro calculates the base size of this structure (without sub-transaction IDs)
- This is part of PostgreSQL's commit timestamp tracking system which is controlled by the `track_commit_timestamp` GUC parameter
- The structure is primarily used internally by the WAL subsystem for recovery and replication purposes