# logicalrep_write_stream_commit

## Location
src/backend/replication/logical/proto.c: 1112 - 1136

## Overview
Writes a stream commit message to the logical replication output stream to signal the successful commit of a streaming transaction.

## Definition
void logicalrep_write_stream_commit(StringInfo out, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)

## Detailed Description
This function is part of PostgreSQL's logical replication protocol implementation for streaming transactions. It writes a LOGICAL_REP_MSG_STREAM_COMMIT message that contains detailed information about the committed transaction. This message is sent after all the changes in a streaming transaction have been transmitted and the transaction has been successfully committed.

The function packages essential transaction metadata including the transaction ID, LSN information, and commit timestamp into the replication stream. This allows subscribers to properly commit the replicated transaction with the correct metadata preserved.

## Parameters / Member Variables
- `out`: StringInfo buffer where the stream commit message will be written
- `txn`: ReorderBufferTXN structure containing transaction information (xid, end_lsn, commit_time)
- `commit_lsn`: XLogRecPtr indicating the LSN where the transaction was committed

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - LOGICAL_REP_MSG_STREAM_COMMIT (message type constant)
  - TransactionIdIsValid (assertion)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (data structure)
- Called from (representative examples):
  - [pgoutput_stream_commit](../p/pgoutput_stream_commit.md)

## Notes and Other Information
- Includes transaction ID, flags field (currently unused), commit LSN, end LSN, and commit timestamp
- Part of the logical replication streaming transaction protocol
- Used to finalize a streaming transaction sequence after all changes have been sent
- Contains an assertion to ensure the transaction ID is valid before sending
- The flags field is reserved for future use and currently set to 0
- Located in src/backend/replication/logical/proto.c:1112-1136