# LogicalRepCommitPreparedTxnData

## Location
[src/include/replication/logicalproto.h:156-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L156-L163)

## Overview
LogicalRepCommitPreparedTxnData is a structure that holds protocol information for committing prepared transactions in PostgreSQL logical replication.

## Definition
```c
typedef struct LogicalRepCommitPreparedTxnData
{
    XLogRecPtr    commit_lsn;
    XLogRecPtr    end_lsn;
    TimestampTz   commit_time;
    TransactionId xid;
    char          gid[GIDSIZE];
} LogicalRepCommitPreparedTxnData;
```

## Detailed Description
This structure contains the essential metadata required for committing prepared transactions in the logical replication protocol. It represents the second phase of a two-phase commit operation, where a previously prepared transaction is finalized and committed. The structure ensures that all necessary information about the commit operation is properly transmitted from the publisher to the subscriber, maintaining transaction consistency across the replication stream.

## Parameters / Member Variables
- `commit_lsn`: The LSN (Log Sequence Number) at which the prepared transaction was committed
- `end_lsn`: The ending LSN of the commit operation
- `commit_time`: The timestamp when the prepared transaction was committed
- `xid`: The transaction ID of the committed prepared transaction
- `gid[GIDSIZE]`: The global identifier string for the prepared transaction being committed, limited by GIDSIZE

## Dependencies
- Types/Constants referenced:
  - XLogRecPtr
  - TimestampTz
  - TransactionId
  - GIDSIZE
- Used by functions:
  - [logicalrep_read_commit_prepared](../l/logicalrep_read_commit_prepared.md)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md)

## Notes and Other Information
This structure is specifically used for the commit phase of two-phase commit operations in logical replication. It works in conjunction with LogicalRepPreparedTxnData to provide complete two-phase commit support, ensuring that distributed transactions can be properly coordinated between publisher and subscriber nodes.