# logicalrep_write_rollback_prepared

## Location
[src/backend/replication/logical/proto.c:304-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L304-L335)

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
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendstring](../p/pq_sendstring.md)
- Called from (representative examples):
  - [pgoutput_rollback_prepared_txn](../p/pgoutput_rollback_prepared_txn.md)

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation for two-phase commit support
- It includes an assertion to ensure the transaction has a valid GID, as this should only be called for two-phase transactions
- The message format includes flags (currently always 0), prepare end LSN, transaction end LSN, prepare time, rollback time, transaction ID, and GID
- Located in src/backend/replication/logical/proto.c:304-335
- Used by logical replication output plugins like pgoutput to send rollback prepared notifications to subscribers
- Sends both the original prepare timestamp and the current rollback timestamp for complete transaction lifecycle tracking

## Simplified Source

```c
void logicalrep_write_rollback_prepared(StringInfo out, ReorderBufferTXN *txn,
                                       XLogRecPtr prepare_end_lsn,
                                       TimestampTz prepare_time) {
    uint8 flags = 0;

    // Write rollback prepared message type
    pq_sendbyte(out, LOGICAL_REP_MSG_ROLLBACK_PREPARED);

    // Transaction must have valid GID for two-phase commit
    Assert(txn->gid != NULL);

    // Send flags and transaction data
    pq_sendbyte(out, flags);
    pq_sendint64(out, prepare_end_lsn);
    pq_sendint64(out, txn->end_lsn);
    pq_sendint64(out, prepare_time);
    pq_sendint64(out, txn->xact_time.commit_time);
    pq_sendint32(out, txn->xid);
    pq_sendstring(out, txn->gid);
}
```