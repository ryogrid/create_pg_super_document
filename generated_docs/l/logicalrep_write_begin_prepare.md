# logicalrep_write_begin_prepare

## Location
[src/backend/replication/logical/proto.c:127-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L127-L144)

## Overview
Writes a BEGIN PREPARE message to the logical replication output stream to signal the start of a prepared transaction.

## Definition
```c
void logicalrep_write_begin_prepare(StringInfo out, ReorderBufferTXN *txn)
```

## Detailed Description
This function serializes a BEGIN PREPARE message for logical replication, which marks the beginning of a prepared transaction in the replication stream. It writes the message type identifier followed by transaction metadata including LSN positions, timing information, transaction ID, and the global transaction identifier (GID). This function is part of PostgreSQL's logical replication protocol for handling two-phase commit transactions.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized message will be written
- `txn`: ReorderBufferTXN structure containing transaction information to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)  
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendstring](../p/pq_sendstring.md)
  - LOGICAL_REP_MSG_BEGIN_PREPARE
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
- Called from (representative examples):
  - [pgoutput_begin_prepare_txn](../p/pgoutput_begin_prepare_txn.md)

## Notes and Other Information
- Sends fixed-size fields first (LSNs, timing, XID) followed by variable-length GID string
- Part of the logical replication protocol for two-phase commit support
- The message format includes final_lsn, end_lsn, prepare_time, xid, and gid fields
- Located in src/backend/replication/logical/proto.c:127-144