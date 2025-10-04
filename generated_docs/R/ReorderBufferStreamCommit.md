# ReorderBufferStreamCommit

## Location
[src/backend/replication/logical/reorderbuffer.c:1925-1988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1925-L1988)

## Overview
ReorderBufferStreamCommit handles the commit processing for transactions that have been partially or fully streamed during logical replication, supporting both regular commits and two-phase commit scenarios.

## Definition
```c
static void ReorderBufferStreamCommit(ReorderBuffer *rb, ReorderBufferTXN *txn)
```

## Detailed Description
This function is responsible for committing transactions that have undergone streaming during logical replication. It handles the final phase of streamed transactions by:

1. First streaming any remaining parts of the transaction via ReorderBufferStreamTXN
2. Determining if the transaction is prepared (two-phase commit) or ready for immediate commit
3. For prepared transactions: sends a stream_prepare message and truncates the transaction while preserving it for the final COMMIT PREPARED
4. For regular transactions: sends a stream_commit message and performs complete cleanup

The function ensures proper handling of both single-phase and two-phase commit protocols in the context of streamed logical replication.

## Parameters / Member Variables
- `rb`: ReorderBuffer pointer - the reorder buffer managing the transaction
- `txn`: ReorderBufferTXN pointer - the transaction being committed, which must have been previously streamed

## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_is_streamed (assertion check)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md) (stream remaining transaction parts)
  - rbtxn_prepared (check if transaction is prepared)
  - [ReorderBufferTruncateTXN](ReorderBufferTruncateTXN.md) (for prepared transactions)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md) (for regular transactions)
  - rb->stream_prepare (callback for prepared transactions)
  - rb->stream_commit (callback for regular transactions)
- Called from (representative examples):
  - [ReorderBufferReplay](ReorderBufferReplay.md)

## Notes and Other Information
- This is a static function within reorderbuffer.c for internal use
- The function includes an assertion that the transaction must be streamed before calling
- For prepared transactions, CheckXidAlive is reset to InvalidTransactionId after processing
- The function supports PostgreSQL's two-phase commit protocol in streamed replication scenarios
- Stream prepare messages are sent even if concurrent abort is detected, following the same pattern as DecodePrepare
- Complete transaction cleanup only occurs for non-prepared transactions; prepared transactions are truncated but preserved for the final commit phase

## Simplified Source

```c
static void
ReorderBufferStreamCommit(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
    // Stream any remaining parts of the transaction
    ReorderBufferStreamTXN(rb, txn);

    if (rbtxn_prepared(txn))
    {
        // Two-phase commit: send stream prepare message
        rb->stream_prepare(rb, txn, txn->final_lsn);

        // Truncate transaction but preserve structure for final commit
        ReorderBufferTruncateTXN(rb, txn, true);

        // Reset transaction liveness check
        CheckXidAlive = InvalidTransactionId;
    }
    else
    {
        // Regular commit: send stream commit and cleanup completely
        rb->stream_commit(rb, txn, txn->final_lsn);
        ReorderBufferCleanupTXN(rb, txn);
    }
}
```