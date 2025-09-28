# WalSndPrepareWrite

## Location
[src/backend/replication/walsender.c:1549-1575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1549-L1575)

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
  - [resetStringInfo](../r/resetStringInfo.md) (resets the output buffer)
  - [pq_sendbyte](../p/pq_sendbyte.md) (sends message type `w`)
  - [pq_sendint64](../p/pq_sendint64.md) (sends LSN and timestamp values)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (context structure)
- Called from (representative examples):
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (sets up logical replication slot)
  - [StartLogicalReplication](../S/StartLogicalReplication.md) (initiates logical replication streaming)

## Notes and Other Information
- The function invalidates the LSN for non-final writes to prevent sync replication confusion from duplicate LSNs
- Send time is reserved but filled in later, similar to physical WAL sending (XLogSendPhysical)
- This is a static function local to walsender.c, used specifically for logical replication callbacks
- The function is part of the logical decoding callback interface and must match the expected signature

## Simplified Source

```c
// Simplified version of WalSndPrepareWrite
static void WalSndPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write) {
    // Prevent sync rep confusion with duplicate LSNs
    if (!last_write)
        lsn = InvalidXLogRecPtr;

    // Reset output buffer and prepare message header
    resetStringInfo(ctx->out);

    // Build logical replication protocol message
    pq_sendbyte(ctx->out, 'w');        // WAL data message type
    pq_sendint64(ctx->out, lsn);       // dataStart LSN
    pq_sendint64(ctx->out, lsn);       // walEnd LSN
    pq_sendint64(ctx->out, 0);         // sendtime placeholder
}
```

Key simplifications made:
- Added clear comments explaining the LSN invalidation logic
- Grouped protocol message construction with descriptive comments
- Preserved all essential protocol requirements
- Maintained lightweight design for potential unused writes