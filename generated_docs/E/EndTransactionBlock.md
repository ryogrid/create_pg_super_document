# EndTransactionBlock

## Location
[src/backend/access/transam/xact.c:3993-4152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3993-L4152)

## Overview
EndTransactionBlock implements the SQL COMMIT command by managing transaction block state transitions, handling various transaction states including subtransactions, aborted transactions, and implicit transactions.

## Definition

```c
bool
EndTransactionBlock(bool chain)
```
## Detailed Description
This complex function handles the execution of COMMIT commands across all possible transaction block states. It implements a comprehensive state machine that manages normal transaction commits, subtransaction hierarchies, aborted transaction rollbacks, and implicit transaction warnings. The function supports both regular COMMIT and COMMIT AND CHAIN operations. For subtransactions, it walks up the transaction hierarchy setting appropriate commit or abort states. When transactions are aborted, COMMIT is treated as ROLLBACK. The function only changes block states, deferring actual transaction work to CommitTransactionCommand() to avoid Portal execution complications. It returns true for successful commits, false for rollbacks.

## Parameters / Member Variables
- `chain`: Boolean indicating whether this is a COMMIT AND CHAIN operation

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - CurrentTransactionState (global variable)
  - TBlockState enumeration values (TBLOCK_INPROGRESS, TBLOCK_END, etc.)
  - ereport (error reporting function)
  - elog (logging function)
  - [BlockStateAsString](../B/BlockStateAsString.md) (utility function for debugging)
  - [errcode](../e/errcode.md) (error code definition)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Called from (representative examples):
  - [PrepareTransactionBlock](../P/PrepareTransactionBlock.md) (at src/backend/access/transam/xact.c:3947)
  - [pa_stream_abort](../p/pa_stream_abort.md) (at src/backend/replication/logical/applyparallelworker.c:1452)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md) (at src/backend/replication/logical/worker.c:2276)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (at src/backend/tcop/utility.c:631)

## Notes and Other Information
- Returns true for COMMIT operations, false for ROLLBACK operations
- Handles complex subtransaction hierarchies by walking parent chains
- Provides special handling for COMMIT AND CHAIN vs regular COMMIT
- Issues warnings for COMMIT outside transaction blocks, errors for COMMIT AND CHAIN
- Treats COMMIT as ROLLBACK when in aborted transaction states
- Prevents COMMIT operations within parallel worker contexts
- Comprehensive state validation with fatal errors for invalid transitions
- Sets the chain flag in transaction state for later use by CommitTransactionCommand()
- Actual transaction commit/abort work is deferred to avoid Portal execution issues

## Simplified Source

```c
bool EndTransactionBlock(bool chain) {
    TransactionState s = CurrentTransactionState;
    bool result = false;

    switch (s->blockState) {
        // Normal transaction in progress - prepare for commit
        case TBLOCK_INPROGRESS:
            s->blockState = TBLOCK_END;
            result = true;
            break;

        // Implicit transaction - warn if no explicit BEGIN
        case TBLOCK_IMPLICIT_INPROGRESS:
            if (chain) {
                ereport(ERROR, "COMMIT AND CHAIN can only be used in transaction blocks");
            } else {
                ereport(WARNING, "there is no transaction in progress");
            }
            s->blockState = TBLOCK_END;
            result = true;
            break;

        // Failed transaction - prepare for rollback
        case TBLOCK_ABORT:
            s->blockState = TBLOCK_ABORT_END;
            break;

        // Active subtransaction - commit all subtransactions and main transaction
        case TBLOCK_SUBINPROGRESS:
            while (s->parent != NULL) {
                if (s->blockState == TBLOCK_SUBINPROGRESS) {
                    s->blockState = TBLOCK_SUBCOMMIT;
                } else {
                    elog(FATAL, "unexpected state in subtransaction");
                }
                s = s->parent;
            }
            if (s->blockState == TBLOCK_INPROGRESS) {
                s->blockState = TBLOCK_END;
            } else {
                elog(FATAL, "unexpected main transaction state");
            }
            result = true;
            break;

        // Aborted subtransaction - treat COMMIT as ROLLBACK
        case TBLOCK_SUBABORT:
            while (s->parent != NULL) {
                if (s->blockState == TBLOCK_SUBINPROGRESS) {
                    s->blockState = TBLOCK_SUBABORT_PENDING;
                } else if (s->blockState == TBLOCK_SUBABORT) {
                    s->blockState = TBLOCK_SUBABORT_END;
                } else {
                    elog(FATAL, "unexpected subtransaction state");
                }
                s = s->parent;
            }
            if (s->blockState == TBLOCK_INPROGRESS) {
                s->blockState = TBLOCK_ABORT_PENDING;
            } else if (s->blockState == TBLOCK_ABORT) {
                s->blockState = TBLOCK_ABORT_END;
            } else {
                elog(FATAL, "unexpected main transaction state");
            }
            break;

        // No transaction in progress
        case TBLOCK_STARTED:
            if (chain) {
                ereport(ERROR, "COMMIT AND CHAIN can only be used in transaction blocks");
            } else {
                ereport(WARNING, "there is no transaction in progress");
            }
            result = true;
            break;

        // Parallel worker context - not allowed
        case TBLOCK_PARALLEL_INPROGRESS:
            ereport(FATAL, "cannot commit during a parallel operation");
            break;

        // Invalid states
        default:
            elog(FATAL, "EndTransactionBlock: unexpected state");
            break;
    }

    // Store chain flag for later use
    s->chain = chain;

    return result; // true for COMMIT, false for ROLLBACK
}
```