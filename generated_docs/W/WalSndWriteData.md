# WalSndWriteData

## Location
[src/backend/replication/walsender.c:1576-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1576-L1617)

## Overview
WalSndWriteData is a callback function that actually sends prepared logical replication data to clients over the network, handling timing, flow control, and timeout management.

## Definition
```c
static void WalSndWriteData(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
```

## Detailed Description
This function serves as the `write` callback for LogicalDecodingContext during logical replication. It completes the data transmission process started by WalSndPrepareWrite by filling in the current timestamp and sending the prepared data to the client via a CopyData packet. The function implements sophisticated flow control by taking either a fast path (when not close to timeout and no pending writes) or a slow path that processes pending writes through ProcessPendingWrites().

The function handles network I/O carefully, checking for interrupts, attempting non-blocking flushes, and managing timeouts to ensure reliable data delivery while maintaining responsiveness to client replies and system events.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the prepared output data and decoding state
- `lsn`: XLogRecPtr representing the LSN of the WAL record being written
- `xid`: TransactionId of the transaction (currently unused in the function)
- `last_write`: Boolean flag indicating if this is the final write in a sequence

## Dependencies
- Functions called/Symbols referenced:
  - resetStringInfo (clears temporary buffer)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (gets current timestamp for protocol)
  - [pq_sendint64](../p/pq_sendint64.md) (formats timestamp data)
  - pq_putmessage_noblock (sends CopyData message)
  - pq_flush_if_writable (attempts non-blocking flush)
  - [WalSndShutdown](WalSndShutdown.md) (shuts down on flush failure)
  - TimestampTzPlusMilliseconds (timeout calculation)
  - pq_is_send_pending (checks for pending output)
  - [ProcessPendingWrites](../P/ProcessPendingWrites.md) (handles slow path processing)
- Called from (representative examples):
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (sets up logical replication slot)
  - [StartLogicalReplication](../S/StartLogicalReplication.md) (initiates logical replication streaming)

## Notes and Other Information
- Fills timestamp as late as possible to maintain protocol compatibility with physical replication
- Uses a two-path approach: fast path for low-latency scenarios, slow path for handling backpressure
- The timeout threshold is half of wal_sender_timeout to provide early warning of potential timeouts
- Implements careful memory management by copying timestamp data into the prepared buffer
- Part of the logical decoding callback interface and must maintain the expected function signature
- Critical for maintaining responsive logical replication while preventing timeout-related disconnections