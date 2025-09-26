# logicalrep_write_commit_prepared

## Location
src/backend/replication/logical/proto.c: 248 - 277

## Overview
Writes a COMMIT PREPARED message to the logical replication output stream for a two-phase commit transaction that is being committed.

## Definition
```c
void logicalrep_write_commit_prepared(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
```

## Detailed Description
This function serializes a COMMIT PREPARED message into the logical replication protocol stream. It is used when a previously prepared two-phase commit transaction is being committed, sending the commit information to logical replication subscribers. The function writes the message type identifier, flags, commit LSN, transaction end LSN, commit timestamp, transaction ID, and global identifier (GID) to the output buffer in the proper binary format expected by the logical replication protocol.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized commit prepared message will be written
- `txn`: ReorderBufferTXN structure containing the transaction information including GID, end LSN, commit time, and transaction ID
- `commit_lsn`: XLogRecPtr specifying the LSN where the commit record was written

## Dependencies
- Functions called/Symbols referenced:
  - LOGICAL_REP_MSG_COMMIT_PREPARED
  - pq_sendbyte
  - pq_sendint64
  - pq_sendint32
  - pq_sendstring
- Called from (representative examples):
  - pgoutput_commit_prepared_txn

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation for two-phase commit support
- It includes an assertion to ensure the transaction has a valid GID, as this should only be called for two-phase transactions
- The message format includes flags (currently always 0), commit LSN, end LSN, commit timestamp, transaction ID, and GID
- Located in src/backend/replication/logical/proto.c:248-277
- Used by logical replication output plugins like pgoutput to send commit prepared notifications to subscribers