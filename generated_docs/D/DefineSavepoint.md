# DefineSavepoint\n\n## Overview\nDefineSavepoint executes a SAVEPOINT command by creating a new subtransaction with an optional name within an active transaction block.\n\n## Definition\nvoid DefineSavepoint(const char *name)\n\n## Detailed Description\nDefineSavepoint implements the SAVEPOINT SQL command functionality by creating a new subtransaction level within an existing transaction block. The function validates that savepoints can only be created within explicit transaction blocks (TBLOCK_INPROGRESS or TBLOCK_SUBINPROGRESS states) and are prohibited in implicit transaction blocks, parallel operations, and parallel worker contexts. When valid, it calls PushTransaction() to create a new transaction state level and optionally assigns a name to the savepoint stored in TopTransactionContext. The restriction against implicit transaction blocks maintains consistency with exec_simple_query's error handling behavior, which abandons the entire query string upon error.\n\n## Parameters / Member Variables\n- name: Optional savepoint name string, can be NULL for anonymous savepoints\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - CurrentTransactionState (global variable)\n  - [IsInParallelMode](../I/IsInParallelMode.md)\n  - IsParallelWorker\n  - [PushTransaction](../P/PushTransaction.md)\n  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)\n  - TopTransactionContext (global variable)\n  - ereport\n  - elog\n  - [BlockStateAsString](../B/BlockStateAsString.md)\n  - Various TBLOCK_* state constants\n- Called from (representative examples):\n  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md)\n  - [pa_start_subtrans](../p/pa_start_subtrans.md)\n  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)\n\n## Notes and Other Information\n- Only allowed within explicit transaction blocks (TBLOCK_INPROGRESS or TBLOCK_SUBINPROGRESS)\n- Prohibited in implicit transaction blocks to maintain consistency with error handling\n- Cannot be used during parallel operations or in parallel worker contexts\n- Savepoint names are stored in TopTransactionContext for persistence\n- Creates a new subtransaction level using PushTransaction()\n- Anonymous savepoints are supported when name parameter is NULL\n- Error messages for implicit transactions are phrased as if no transaction block exists

## Simplified Source

```c
// Simplified version of DefineSavepoint
void DefineSavepoint(const char *name) {
    TransactionState s = CurrentTransactionState;

    // Check if parallel operations are active - savepoints not allowed
    if (IsInParallelMode() || IsParallelWorker()) {
        ereport(ERROR, "cannot define savepoints during a parallel operation");
    }

    switch (s->blockState) {
        case TBLOCK_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
            // Valid transaction states - create new subtransaction
            PushTransaction();
            s = CurrentTransactionState;  // Update to new transaction state

            // Store savepoint name in persistent context if provided
            if (name) {
                s->name = MemoryContextStrdup(TopTransactionContext, name);
            }
            break;

        case TBLOCK_IMPLICIT_INPROGRESS:
            // Implicit transactions don't support savepoints
            ereport(ERROR, "SAVEPOINT can only be used in transaction blocks");
            break;

        default:
            // All other states are invalid for savepoint creation
            elog(FATAL, "DefineSavepoint: unexpected state %s",
                 BlockStateAsString(s->blockState));
            break;
    }
}
```

Key simplifications made:
- Condensed error reporting calls to show essential error messages
- Grouped all valid transaction states together (TBLOCK_INPROGRESS/TBLOCK_SUBINPROGRESS)
- Consolidated all invalid states into a single default case
- Removed detailed comments about design rationale, keeping only functional comments
- Simplified variable names and logic flow while preserving core algorithm
- Maintained the three main code paths: parallel check, valid states, and error cases