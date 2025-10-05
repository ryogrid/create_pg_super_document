# pgoutput_begin_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:574-587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L574-L587)

## Overview
The transaction begin callback for the pgoutput plugin that prepares transaction-specific data structures but delays sending the BEGIN message until the first actual change.

## Definition
```c
static void pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
```

## Detailed Description
This callback function is invoked when a transaction begins during logical replication. Rather than immediately sending a BEGIN message to subscribers, it implements a deferred approach to optimize bandwidth usage. The function allocates and initializes transaction-specific data storage but postpones the actual BEGIN message transmission until the first change occurs within the transaction. This design prevents empty transactions (those with no changes to published tables) from generating unnecessary BEGIN/COMMIT message pairs, which would waste network bandwidth without providing meaningful replication data. The function simply sets up the necessary data structures to track the transaction state.

## Parameters / Member Variables
- `ctx`: Logical decoding context containing replication state and configuration
- `txn`: ReorderBuffer transaction structure representing the transaction being processed

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (logical decoding context structure)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (transaction buffer structure)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (zero-initialized memory allocation function)
  - [PGOutputTxnData](../P/PGOutputTxnData.md) (transaction-specific data structure)
- Called from:
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as begin_cb callback)

## Notes and Other Information
- Implements bandwidth optimization by deferring BEGIN message transmission
- Prevents empty transactions from generating unnecessary network traffic
- Allocates transaction-specific data in the appropriate memory context
- Uses zero-initialized allocation to ensure clean initial state
- The actual BEGIN message is sent later when the first change is processed
- Part of the pgoutput plugin's efficiency optimization strategy
- Critical for scenarios where only a subset of tables are replicated
- The deferred BEGIN approach reduces overhead for transactions with no published changes

## Simplified Source

```c
static void pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    // Allocate transaction-specific data but don't send BEGIN yet
    // This optimizes bandwidth by deferring BEGIN until first actual change
    PGOutputTxnData *txndata = MemoryContextAllocZero(ctx->context, sizeof(PGOutputTxnData));
    txn->output_plugin_private = txndata;
}
```