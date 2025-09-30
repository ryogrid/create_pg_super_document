# ReleaseSavepoint\n\n## Overview\nReleaseSavepoint executes a RELEASE SAVEPOINT command by marking subtransactions for commit up to the specified savepoint level within the transaction hierarchy.\n\n## Definition\nvoid ReleaseSavepoint(const char *name)\n\n## Detailed Description\nReleaseSavepoint implements the RELEASE SAVEPOINT SQL command functionality by locating a named savepoint in the transaction hierarchy and marking all subtransactions from the current level up to the target savepoint for commit. The function validates that it can only be called within explicit subtransactions (TBLOCK_SUBINPROGRESS state) and prohibits use in implicit transactions and parallel operations. It searches up the transaction stack to find the named savepoint, ensures it exists within the current savepoint level boundaries, then marks each subtransaction from the current level to the target with TBLOCK_SUBRELEASE state. The actual commit processing is deferred to CommitTransactionCommand(). Like other transaction commands, this function only manages state transitions rather than performing actual transaction operations.\n\n## Parameters / Member Variables\n- name: Name of the savepoint to release (required, not NULL)\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - CurrentTransactionState (global variable)\n  - [IsInParallelMode](../I/IsInParallelMode.md)\n  - IsParallelWorker\n  - PointerIsValid\n  - strcmp\n  - ereport\n  - elog\n  - [BlockStateAsString](../B/BlockStateAsString.md)\n  - Assert\n  - Various TBLOCK_* state constants\n- Called from (representative examples):\n  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)\n\n## Notes and Other Information\n- Only valid within explicit subtransactions (TBLOCK_SUBINPROGRESS state)\n- Prohibited in implicit transaction blocks and parallel operations\n- Searches transaction hierarchy upward to locate named savepoint\n- Enforces savepoint level boundaries to prevent crossing levels\n- Marks all subtransactions from current to target with TBLOCK_SUBRELEASE\n- Actual commit work is deferred to CommitTransactionCommand()\n- Generates specific error messages for missing savepoints and level violations\n- Validates savepoint existence at the correct nesting level

## Simplified Source

```c
void ReleaseSavepoint(const char *name) {
    TransactionState s = CurrentTransactionState;
    TransactionState target, xact;

    // Prevent use in parallel operations
    if (IsInParallelMode() || IsParallelWorker())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                       errmsg("cannot release savepoints during a parallel operation")));

    // Validate current transaction state
    switch (s->blockState) {
        case TBLOCK_INPROGRESS:
            // No savepoint exists
            ereport(ERROR, (errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
                           errmsg("savepoint \"%s\" does not exist", name)));
            break;

        case TBLOCK_IMPLICIT_INPROGRESS:
            // Not in explicit transaction block
            ereport(ERROR, (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
                           errmsg("RELEASE SAVEPOINT can only be used in transaction blocks")));
            break;

        case TBLOCK_SUBINPROGRESS:
            // Valid state for releasing savepoint
            break;

        default:
            // All other states are invalid
            elog(FATAL, "ReleaseSavepoint: unexpected state %s",
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

    // Mark all subtransactions up to target for commit
    xact = CurrentTransactionState;
    for (;;) {
        Assert(xact->blockState == TBLOCK_SUBINPROGRESS);
        xact->blockState = TBLOCK_SUBRELEASE;  // Mark for release
        if (xact == target)
            break;
        xact = xact->parent;
        Assert(PointerIsValid(xact));
    }
}
```