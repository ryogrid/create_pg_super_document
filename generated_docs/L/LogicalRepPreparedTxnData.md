# LogicalRepPreparedTxnData

## Location
[src/include/replication/logicalproto.h:144-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L144-L151)

## Overview
LogicalRepPreparedTxnData is a structure that holds protocol information for prepared transactions in PostgreSQL logical replication, specifically used for begin_prepare and prepare messages.

## Definition

```c
typedef struct LogicalRepPreparedTxnData
{
	XLogRecPtr	prepare_lsn;
	XLogRecPtr	end_lsn;
	TimestampTz prepare_time;
	TransactionId xid;
	char		gid[GIDSIZE];
} LogicalRepPreparedTxnData;
```
## Detailed Description
This structure encapsulates essential information about prepared transactions in the logical replication protocol. It serves as a container for transaction metadata that needs to be transmitted between the publisher and subscriber during two-phase commit operations. The structure is used when handling prepared transaction events, allowing the logical replication system to maintain consistency across distributed transactions.

## Parameters / Member Variables
- `prepare_lsn`: The LSN (Log Sequence Number) at which the transaction was prepared
- `end_lsn`: The ending LSN of the prepared transaction
- `prepare_time`: The timestamp when the transaction was prepared
- `xid`: The transaction ID of the prepared transaction
- `gid[GIDSIZE]`: The global identifier string for the prepared transaction, limited by GIDSIZE
## Dependencies
- Types/Constants referenced:
  - XLogRecPtr
  - TimestampTz
  - TransactionId
  - GIDSIZE
- Used by functions:
  - [logicalrep_read_begin_prepare](../l/logicalrep_read_begin_prepare.md)
  - [logicalrep_read_prepare_common](../l/logicalrep_read_prepare_common.md)
  - [logicalrep_read_prepare](../l/logicalrep_read_prepare.md)
  - [logicalrep_read_stream_prepare](../l/logicalrep_read_stream_prepare.md)
  - [apply_handle_begin_prepare](../a/apply_handle_begin_prepare.md)
  - [apply_handle_prepare_internal](../a/apply_handle_prepare_internal.md)
  - [apply_handle_prepare](../a/apply_handle_prepare.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)

## Notes and Other Information
This structure is part of the logical replication protocol implementation and is essential for supporting two-phase commit in logical replication scenarios. It ensures that prepared transaction information is properly communicated between replication participants, maintaining ACID properties in distributed environments.