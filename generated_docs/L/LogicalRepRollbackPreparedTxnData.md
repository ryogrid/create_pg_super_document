# LogicalRepRollbackPreparedTxnData

## Location
[src/include/replication/logicalproto.h:173-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L173-L181)

## Overview
LogicalRepRollbackPreparedTxnData is a structure that holds protocol information for rolling back prepared transactions in PostgreSQL logical replication, including additional metadata to ensure proper rollback validation.

## Definition
```c
typedef struct LogicalRepRollbackPreparedTxnData
{
    XLogRecPtr    prepare_end_lsn;
    XLogRecPtr    rollback_end_lsn;
    TimestampTz   prepare_time;
    TimestampTz   rollback_time;
    TransactionId xid;
    char          gid[GIDSIZE];
} LogicalRepRollbackPreparedTxnData;
```

## Detailed Description
This structure contains metadata for rolling back prepared transactions in logical replication. It includes both prepare and rollback information to enable proper validation on the downstream node. The prepare_end_lsn and prepare_time are specifically used to verify whether the downstream node has received the corresponding prepared transaction before applying the rollback. This validation is crucial because the global identifier (gid) alone is insufficient, as downstream nodes may have prepared transactions with the same identifier from different sources.

## Parameters / Member Variables
- `prepare_end_lsn`: The ending LSN of the original prepare operation, used for validation
- `rollback_end_lsn`: The ending LSN of the rollback operation
- `prepare_time`: The timestamp when the transaction was originally prepared, used for validation
- `rollback_time`: The timestamp when the prepared transaction was rolled back
- `xid`: The transaction ID of the prepared transaction being rolled back
- `gid[GIDSIZE]`: The global identifier string for the prepared transaction being rolled back, limited by GIDSIZE

## Dependencies
- Types/Constants referenced:
  - XLogRecPtr
  - TimestampTz
  - TransactionId
  - GIDSIZE
- Used by functions:
  - logicalrep_read_rollback_prepared
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md)

## Notes and Other Information
This structure is designed with enhanced validation capabilities compared to simple rollback operations. The inclusion of both prepare and rollback timestamps, along with LSN information, ensures that rollback operations are only applied to the correct prepared transactions. This prevents issues where multiple prepared transactions might share the same global identifier across different replication streams or time periods.