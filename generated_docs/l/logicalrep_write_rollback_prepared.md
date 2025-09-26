# logicalrep_write_rollback_prepared

## Location
src/backend/replication/logical/proto.c: 304 - 335

## Overview
Writes a ROLLBACK PREPARED message to the logical replication output stream for a two-phase commit transaction that is being rolled back.

## Definition
```c
void logicalrep_write_rollback_prepared(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr prepare_end_lsn, TimestampTz prepare_time)
```

## Detailed Description
This function serializes a ROLLBACK PREPARED message into the logical replication protocol stream. It is used when a previously prepared two-phase commit transaction is being rolled back, sending the rollback information to logical replication subscribers. The function writes the message type identifier, flags, prepare end LSN, transaction end LSN, prepare timestamp, rollback timestamp, transaction ID, and global identifier (GID) to the output buffer in the proper binary format expected by the logical replication protocol.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized rollback prepared message will be written
- `txn`: ReorderBufferTXN structure containing the transaction information including GID, end LSN, rollback time, and transaction ID
- `prepare_end_lsn`: XLogRecPtr specifying the LSN where the original prepare record ended
- `prepare_time`: TimestampTz specifying when the transaction was originally prepared

## Dependencies
- Functions called/Symbols referenced:
  - LOGICAL_REP_MSG_ROLLBACK_PREPARED
  - pq_sendbyte
  - pq_sendint64
  - pq_sendint32
  - pq_sendstring
- Called from (representative examples):
  - pgoutput_rollback_prepared_txn

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation for two-phase commit support
- It includes an assertion to ensure the transaction has a valid GID, as this should only be called for two-phase transactions
- The message format includes flags (currently always 0), prepare end LSN, transaction end LSN, prepare time, rollback time, transaction ID, and GID
- Located in src/backend/replication/logical/proto.c:304-335
- Used by logical replication output plugins like pgoutput to send rollback prepared notifications to subscribers
- Sends both the original prepare timestamp and the current rollback timestamp for complete transaction lifecycle tracking