# ReorderBufferProcessTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:2127-2514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L2127-L2514)

## Overview
Core helper function that processes and replays all changes in a transaction (and its subtransactions) in LSN order, supporting both regular replay and streaming modes for logical replication.

## Definition
```c
static void ReorderBufferProcessTXN(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                   XLogRecPtr commit_lsn, volatile Snapshot snapshot_now,
                                   volatile CommandId command_id, bool streaming)
```

## Detailed Description
ReorderBufferProcessTXN is the central engine for transaction processing in PostgreSQL's logical replication system. It performs a k-way merge of changes from the main transaction and all subtransactions, processing them in LSN order to maintain consistency. The function handles multiple types of changes including INSERT/UPDATE/DELETE operations, truncates, messages, invalidations, snapshots, and command ID updates. It supports both streaming and non-streaming modes, manages toast data reconstruction, handles speculative insertions, and maintains proper transaction isolation through snapshot management. The function also includes comprehensive error handling and resource cleanup mechanisms.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance managing the replication state and callbacks
- `txn`: Main transaction to process along with all its subtransactions
- `commit_lsn`: LSN of the commit record for this transaction
- `snapshot_now`: Current snapshot for visibility determination (marked volatile for PG_TRY)
- `command_id`: Current command ID for proper tuple visibility (marked volatile for PG_TRY)
- `streaming`: Boolean flag indicating whether to use streaming API instead of regular replay

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferBuildTupleCidHash](ReorderBufferBuildTupleCidHash.md) (build tuple command ID hash)
  - [SetupHistoricSnapshot](../S/SetupHistoricSnapshot.md) (setup snapshot for decoding)
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md)/ReorderBufferIterTXNNext (transaction iteration)
  - [ReorderBufferApplyChange](ReorderBufferApplyChange.md) (apply individual changes)
  - [ReorderBufferApplyMessage](ReorderBufferApplyMessage.md) (apply messages)
  - [ReorderBufferApplyTruncate](ReorderBufferApplyTruncate.md) (apply truncate operations)
  - [ReorderBufferToastReplace](ReorderBufferToastReplace.md)/ReorderBufferToastReset (toast handling)
  - Various relation and snapshot management functions
- Called from (representative examples):
  - [ReorderBufferReplay](ReorderBufferReplay.md) (regular transaction replay)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md) (streaming transaction processing)

## Notes and Other Information
- This is the core processing engine for logical replication in PostgreSQL
- Implements k-way merge algorithm to process changes from multiple subtransactions in proper order
- Handles complex scenarios like speculative insertions, toast data reconstruction, and catalog changes
- Uses PostgreSQL's internal transaction system for proper resource management and error handling
- Supports both streaming and batch processing modes for different replication scenarios
- Includes extensive error checking and validation to ensure data consistency
- The function is marked with volatile parameters due to PG_TRY exception handling requirements
- Critical for maintaining transactional consistency and proper ordering in logical replication streams

## Simplified Source

```c
static void ReorderBufferProcessTXN(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                   XLogRecPtr commit_lsn, volatile Snapshot snapshot_now,
                                   volatile CommandId command_id, bool streaming)
{
    bool using_subtxn;
    ReorderBufferIterTXNState *iterstate = NULL;
    XLogRecPtr prev_lsn = InvalidXLogRecPtr;
    ReorderBufferChange *specinsert = NULL;
    bool stream_started = false;

    // Build tuple command ID hash for catalog lookups
    ReorderBufferBuildTupleCidHash(rb, txn);

    // Setup historic snapshot for decoding
    SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash);

    // Start transaction context for decoding
    using_subtxn = IsTransactionOrTransactionBlock();

    PG_TRY();
    {
        // Begin transaction/subtransaction
        if (using_subtxn)
            BeginInternalSubTransaction(streaming ? "stream" : "replay");
        else
            StartTransactionCommand();

        // Send begin callback for non-streaming transactions
        if (!streaming) {
            if (rbtxn_prepared(txn))
                rb->begin_prepare(rb, txn);
            else
                rb->begin(rb, txn);
        }

        // Initialize transaction iterator and process all changes
        ReorderBufferIterTXNInit(rb, txn, &iterstate);
        while ((change = ReorderBufferIterTXNNext(rb, iterstate)) != NULL) {
            CHECK_FOR_INTERRUPTS();

            // Start streaming on first change
            if (prev_lsn == InvalidXLogRecPtr && streaming) {
                txn->origin_id = change->origin_id;
                rb->stream_start(rb, txn, change->lsn);
                stream_started = true;
            }

            // Ensure proper LSN ordering
            Assert(prev_lsn == InvalidXLogRecPtr || prev_lsn <= change->lsn);
            prev_lsn = change->lsn;

            // Set current xid for concurrent abort detection
            if (streaming || rbtxn_prepared(change->txn)) {
                SetupCheckXidLive(change->txn->xid);
            }

            // Process change based on type
            switch (change->action) {
                case REORDER_BUFFER_CHANGE_INSERT:
                case REORDER_BUFFER_CHANGE_UPDATE:
                case REORDER_BUFFER_CHANGE_DELETE:
                    // Handle DML changes: validate relation, apply change
                    ProcessDMLChange(rb, txn, change, streaming);
                    break;

                case REORDER_BUFFER_CHANGE_TRUNCATE:
                    // Handle truncate operations
                    ProcessTruncateChange(rb, txn, change, streaming);
                    break;

                case REORDER_BUFFER_CHANGE_MESSAGE:
                    // Handle logical replication messages
                    ReorderBufferApplyMessage(rb, txn, change, streaming);
                    break;

                case REORDER_BUFFER_CHANGE_INVALIDATION:
                    // Execute invalidation messages
                    ReorderBufferExecuteInvalidations(change->data.inval.ninvalidations,
                                                    change->data.inval.invalidations);
                    break;

                case REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT:
                    // Update snapshot for visibility
                    UpdateHistoricSnapshot(rb, txn, change, &snapshot_now, command_id);
                    break;

                case REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID:
                    // Update command ID for tuple visibility
                    UpdateCommandId(rb, txn, change, &snapshot_now, &command_id);
                    break;

                // Handle speculative insertion cases
                case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT:
                case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM:
                case REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT:
                    ProcessSpeculativeInsertion(rb, change, &specinsert);
                    break;
            }
        }

        // Finalize transaction processing
        if (iterstate)
            ReorderBufferIterTXNFinish(rb, iterstate);

        // Cleanup any remaining speculative insertion
        if (specinsert != NULL)
            ReorderBufferReturnChange(rb, specinsert, true);

    }
    PG_CATCH();
    {
        // Error cleanup and re-throw
        if (stream_started)
            rb->stream_abort(rb, txn, prev_lsn);

        ReorderBufferResetTXN(rb, txn, snapshot_now, command_id, prev_lsn, specinsert);
        PG_RE_THROW();
    }
    PG_END_TRY();
}
```