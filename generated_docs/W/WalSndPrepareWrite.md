# WalSndPrepareWrite

## Location
src/backend/replication/walsender.c: 1549 - 1575

## Overview
WalSndPrepareWrite is a callback function used in logical decoding to prepare a StringInfo buffer for writing WAL data to logical replication clients.

## Definition
```c
static void WalSndPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
```

## Detailed Description
This function serves as the `prepare_write` callback for LogicalDecodingContext during logical replication. It initializes a StringInfo buffer with the proper protocol headers before logical decoding data is written. The function prepares the output buffer by resetting it and writing the initial protocol message structure including message type, LSN positions, and a placeholder for send time.

The function is designed to be lightweight since the prepared data might not actually be sent if subsequent processing determines the write is unnecessary. It follows the PostgreSQL logical replication protocol by sending a `w` (WAL data) message type followed by dataStart and walEnd LSN values.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the output StringInfo buffer and decoding state
- `lsn`: XLogRecPtr representing the LSN of the WAL record being processed
- `xid`: TransactionId of the transaction (currently unused in the function)
- `last_write`: Boolean flag indicating if this is the final write in a sequence; affects LSN handling for sync replication

## Dependencies
- Functions called/Symbols referenced:
  - resetStringInfo (resets the output buffer)
  - pq_sendbyte (sends message type `w`)
  - pq_sendint64 (sends LSN and timestamp values)
  - LogicalDecodingContext (context structure)
- Called from (representative examples):
  - CreateReplicationSlot (sets up logical replication slot)
  - StartLogicalReplication (initiates logical replication streaming)

## Notes and Other Information
- The function invalidates the LSN for non-final writes to prevent sync replication confusion from duplicate LSNs
- Send time is reserved but filled in later, similar to physical WAL sending (XLogSendPhysical)
- This is a static function local to walsender.c, used specifically for logical replication callbacks
- The function is part of the logical decoding callback interface and must match the expected signature