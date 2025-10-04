# ReorderBufferStreamTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:4185-4301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4185-L4301)

## Overview
Sends data of a large transaction (and its subtransactions) to the output plugin using the streaming API instead of waiting for commit, enabling processing of large transactions without memory exhaustion.

## Definition
```c
static void ReorderBufferStreamTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
```

## Detailed Description
This function implements the streaming mechanism for large in-progress transactions, allowing logical replication to process changes incrementally rather than buffering everything until commit. It handles complex snapshot management to ensure consistency when streaming transaction changes.

Key responsibilities include:
- **Snapshot Management**: For first-time streaming, it walks through all subtransactions to build an appropriate snapshot. For subsequent streaming runs, it reuses and updates the existing snapshot.
- **Subtransaction Processing**: Transfers snapshots from subtransactions to the parent transaction to ensure catalog changes are visible.
- **Change Processing**: Calls ReorderBufferProcessTXN to actually process and send changes to the output plugin via the streaming API.
- **Statistics Tracking**: Updates streaming statistics including count, bytes, and transaction numbers.
- **State Management**: Maintains transaction state and ensures proper cleanup after streaming.

The function implements sophisticated logic to handle both initial streaming (snapshot_now == NULL) and continuation streaming for transactions that have been partially streamed before. This enables efficient processing of very large transactions that would otherwise exceed memory limits.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance containing global state and configuration
- `txn`: Top-level ReorderBufferTXN to be streamed (must not be a subtransaction)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTransferSnapToParent](ReorderBufferTransferSnapToParent.md) (snapshot management for subtransactions)
  - [ReorderBufferCopySnap](ReorderBufferCopySnap.md) (snapshot copying and management)
  - [ReorderBufferFreeSnap](ReorderBufferFreeSnap.md) (snapshot cleanup)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (actual change processing and output)
  - [UpdateDecodingStats](../U/UpdateDecodingStats.md) (statistics maintenance)
  - rbtxn_is_toptxn/rbtxn_is_streamed (transaction state checks)
- Called from (representative examples):
  - [ReorderBufferCheckMemoryLimit](ReorderBufferCheckMemoryLimit.md) (when choosing streaming over spilling)
  - [ReorderBufferStreamCommit](ReorderBufferStreamCommit.md) (during commit processing)
  - [ReorderBufferProcessPartialChange](ReorderBufferProcessPartialChange.md) (partial change streaming)

## Notes and Other Information
- Only works with top-level transactions (subtransactions are not directly streamable)
- Implements complex snapshot management for transaction consistency
- Handles both initial streaming and continuation of previously streamed transactions
- Updates comprehensive statistics for monitoring streaming performance
- The streaming API differs from the standard commit-time processing API
- [Snapshot](../S/Snapshot.md) handling is more complex than regular commit processing due to in-progress state
- Ensures all changes and memory are properly cleaned up after streaming
- Critical for handling large transactions that would otherwise cause memory issues

## Simplified Source

```c
static void
ReorderBufferStreamTXN(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    Snapshot snapshot_now;
    CommandId command_id;
    Size stream_bytes;
    bool txn_is_streamed;

    Assert(rbtxn_is_toptxn(txn));

    // Handle snapshot management for streaming
    if (txn->snapshot_now == NULL)
    {
        // First time streaming - build snapshot from subtransactions
        dlist_iter subxact_i;

        Assert(!rbtxn_is_streamed(txn));
        Assert(txn->command_id == InvalidCommandId);

        // Transfer snapshots from all subtransactions
        dlist_foreach(subxact_i, &txn->subtxns)
        {
            ReorderBufferTXN *subtxn = dlist_container(ReorderBufferTXN, node, subxact_i.cur);
            ReorderBufferTransferSnapToParent(txn, subtxn);
        }

        // Create initial snapshot if transaction has changes
        if (txn->base_snapshot == NULL)
        {
            Assert(txn->ninvalidations == 0);
            return;
        }

        command_id = FirstCommandId;
        snapshot_now = ReorderBufferCopySnap(rb, txn->base_snapshot, txn, command_id);
    }
    else
    {
        // Continuation streaming - reuse existing snapshot
        Assert(rbtxn_is_streamed(txn));

        command_id = txn->command_id;
        snapshot_now = ReorderBufferCopySnap(rb, txn->snapshot_now, txn, command_id);

        // Free previous snapshot
        Assert(txn->snapshot_now->copied);
        ReorderBufferFreeSnap(rb, txn->snapshot_now);
        txn->snapshot_now = NULL;
    }

    // Process and stream the transaction changes
    txn_is_streamed = rbtxn_is_streamed(txn);
    stream_bytes = txn->total_size;

    ReorderBufferProcessTXN(rb, txn, InvalidXLogRecPtr, snapshot_now, command_id, true);

    // Update streaming statistics
    rb->streamCount += 1;
    rb->streamBytes += stream_bytes;
    rb->streamTxns += (txn_is_streamed) ? 0 : 1;

    UpdateDecodingStats((LogicalDecodingContext *) rb->private_data);

    // Verify cleanup completed
    Assert(dlist_is_empty(&txn->changes));
    Assert(txn->nentries == 0);
    Assert(txn->nentries_mem == 0);
}
```