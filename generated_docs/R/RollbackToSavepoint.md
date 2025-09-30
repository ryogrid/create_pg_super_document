# RollbackToSavepoint

## Location
[src/backend/access/transam/xact.c:4516-4642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L4516-L4642)

## Overview
RollbackToSavepoint executes a ROLLBACK TO <savepoint> command by marking subtransactions for abort and setting the target savepoint for restart, without performing actual transaction operations immediately.

## Definition

```c
void
RollbackToSavepoint(const char *name)
```
## Detailed Description
RollbackToSavepoint implements the PostgreSQL ROLLBACK TO SAVEPOINT SQL command functionality. Rather than immediately performing transaction rollback operations, it manages the transaction state machine by marking subtransactions between the current state and the target savepoint as "abort pending" and the target savepoint as "restart pending". The actual rollback work is deferred to CommitTransactionCommand.

The function performs several critical validations:
- Prevents rollback to savepoints during parallel operations 
- Ensures savepoints exist and are accessible within the current transaction context
- Validates that the target savepoint exists within the current savepoint level
- Enforces proper transaction block state requirements

The state changes follow a specific pattern: subtransactions between current and target are marked as TBLOCK_SUBABORT_PENDING or TBLOCK_SUBABORT_END, while the target savepoint is marked as TBLOCK_SUBRESTART or TBLOCK_SUBABORT_RESTART depending on its current state.

## Parameters / Member Variables
- : The name of the savepoint to roll back to, as specified in the ROLLBACK TO SAVEPOINT command

## Dependencies
- Functions called/Symbols referenced:
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - IsParallelWorker
  - ereport
  - PointerIsValid
  - [BlockStateAsString](../B/BlockStateAsString.md)
  - elog
- Transaction state constants:
  - TBLOCK_INPROGRESS, TBLOCK_ABORT, TBLOCK_IMPLICIT_INPROGRESS
  - TBLOCK_SUBINPROGRESS, TBLOCK_SUBABORT
  - TBLOCK_SUBABORT_PENDING, TBLOCK_SUBABORT_END
  - TBLOCK_SUBRESTART, TBLOCK_SUBABORT_RESTART
- Called from:
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main SQL command processing)
  - [pa_stream_abort](../p/pa_stream_abort.md) (parallel apply worker error handling)

## Notes and Other Information
- This function only changes the transaction block state; actual abort operations are performed later by CommitTransactionCommand
- Parallel operations are strictly prohibited during savepoint rollback to maintain transaction consistency
- The function traverses the transaction state stack to find the named savepoint and validate accessibility
- Savepoint level boundaries cannot be crossed, ensuring proper nesting semantics
- Error handling includes specific error codes for invalid specifications and transaction states

## Simplified Source

```c
void RollbackToSavepoint(const char *name) {
    TransactionState s = CurrentTransactionState;
    TransactionState target, xact;

    // Prevent rollback to savepoints during parallel operations
    if (IsInParallelMode() || IsParallelWorker())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                       errmsg("cannot rollback to savepoints during a parallel operation")));

    // Validate current transaction state
    switch (s->blockState) {
        case TBLOCK_INPROGRESS:
        case TBLOCK_ABORT:
            // No savepoint exists
            ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
                           errmsg("savepoint \"%s\" does not exist", name)));
            break;

        case TBLOCK_IMPLICIT_INPROGRESS:
            // Not in explicit transaction block
            ereport(ERROR, (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                           errmsg("ROLLBACK TO SAVEPOINT can only be used in transaction blocks")));
            break;

        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_SUBABORT:
            // Valid states for rollback to savepoint
            break;

        default:
            // All other states are invalid
            elog(FATAL, "RollbackToSavepoint: unexpected state %s",
                 BlockStateAsString(s->blockState));
            break;
    }

    // Search for the named savepoint in the transaction hierarchy
    for (target = s; PointerIsValid(target); target = target->parent) {
        if (PointerIsValid(target->name) && strcmp(target->name, name) == 0)
            break;
    }

    // Validate savepoint was found
    if (!PointerIsValid(target))
        ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
                       errmsg("savepoint \"%s\" does not exist", name)));

    // Ensure savepoint is at the correct nesting level
    if (target->savepointLevel != s->savepointLevel)
        ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
                       errmsg("savepoint \"%s\" does not exist within current savepoint level", name)));

    // Mark all subtransactions up to target for abort
    xact = CurrentTransactionState;
    for (;;) {
        if (xact == target)
            break;

        // Set appropriate abort pending state
        if (xact->blockState == TBLOCK_SUBINPROGRESS)
            xact->blockState = TBLOCK_SUBABORT_PENDING;
        else if (xact->blockState == TBLOCK_SUBABORT)
            xact->blockState = TBLOCK_SUBABORT_END;
        else
            elog(FATAL, "RollbackToSavepoint: unexpected state %s",
                 BlockStateAsString(xact->blockState));

        xact = xact->parent;
        Assert(PointerIsValid(xact));
    }

    // Mark the target savepoint for restart
    if (xact->blockState == TBLOCK_SUBINPROGRESS)
        xact->blockState = TBLOCK_SUBRESTART;
    else if (xact->blockState == TBLOCK_SUBABORT)
        xact->blockState = TBLOCK_SUBABORT_RESTART;
    else
        elog(FATAL, "RollbackToSavepoint: unexpected state %s",
             BlockStateAsString(xact->blockState));
}
```