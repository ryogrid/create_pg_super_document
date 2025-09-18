# LogicalRepStreamAbortData

## Location
src/include/replication/logicalproto.h: 186 - 192

## Overview
LogicalRepStreamAbortData is a structure that holds transaction protocol information for stream abort operations in PostgreSQL logical replication.

## Definition
```c
typedef struct LogicalRepStreamAbortData
{
    TransactionId xid;
    TransactionId subxid;
    XLogRecPtr    abort_lsn;
    TimestampTz   abort_time;
} LogicalRepStreamAbortData;
```

## Detailed Description
This structure encapsulates the essential information required for handling stream abort operations in logical replication. Stream abort operations occur when a streaming transaction needs to be aborted before completion. The structure contains both the main transaction ID and subtransaction ID, allowing for proper handling of nested transaction aborts within the streaming replication context. This is particularly important for maintaining consistency when large transactions are being streamed and need to be rolled back.

## Parameters / Member Variables
- `xid`: The main transaction ID of the transaction being aborted
- `subxid`: The subtransaction ID within the main transaction that is being aborted
- `abort_lsn`: The LSN (Log Sequence Number) at which the stream abort occurred
- `abort_time`: The timestamp when the stream abort operation took place

## Dependencies
- Types/Constants referenced:
  - TransactionId
  - XLogRecPtr
  - TimestampTz
- Used by functions:
  - [pa_stream_abort](../p/pa_stream_abort.md)
  - [logicalrep_read_stream_abort](../l/logicalrep_read_stream_abort.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)

## Notes and Other Information
This structure is specifically designed for streaming replication scenarios where large transactions are transmitted incrementally. The presence of both main transaction ID and subtransaction ID allows for fine-grained control over which parts of a complex transaction hierarchy should be aborted. This is essential for maintaining ACID properties even when dealing with partial transaction streams that need to be rolled back.