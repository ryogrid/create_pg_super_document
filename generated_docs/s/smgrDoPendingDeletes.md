# smgrDoPendingDeletes

## Location
[src/backend/catalog/storage.c:657-724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L657-L724)

## Overview
smgrDoPendingDeletes executes relation file deletions that were deferred until transaction end, handling both commit and abort scenarios.

## Definition
```c
void smgrDoPendingDeletes(bool isCommit)
```

## Detailed Description
This function processes the list of pending relation deletions that were queued during the transaction. It operates at transaction boundaries (commit or abort) and also when aborting subtransactions to ensure immediate cleanup of failed operations. The function filters pending deletions based on the current transaction nesting level and whether the deletion should occur at commit or abort.

For each applicable deletion, it opens the relation using the storage manager, collects them into an array, and then performs batch unlinking using smgrdounlinkall(). This approach is more efficient than deleting relations one by one. The function handles cases where relations may have no physical storage, such as temporary relations that were already cleaned up by RemovePgTempFiles.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether this is being called at commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [smgropen](smgropen.md)
  - [smgrdounlinkall](smgrdounlinkall.md)
  - [smgrclose](smgrclose.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AtSubAbort_smgr](../A/AtSubAbort_smgr.md)

## Notes and Other Information
- The function handles transaction nesting by only processing entries at the current or deeper nesting levels
- Uses dynamic array allocation that starts at size 8 and doubles when needed for efficient batch processing
- Entries are unlinked from the pending list before processing to avoid retry attempts on failure
- Can handle relations that have no physical storage without error
- The atCommit flag in PendingRelDelete determines whether deletion occurs at commit or abort

## Simplified Source

```c
// Simplified version of smgrDoPendingDeletes
void smgrDoPendingDeletes(bool isCommit) {
    int nestLevel = GetCurrentTransactionNestLevel();
    PendingRelDelete *pending, *prev = NULL, *next;
    int nrels = 0, maxrels = 0;
    SMgrRelation *srels = NULL;

    // Walk through pending deletion list
    for (pending = pendingDeletes; pending != NULL; pending = next) {
        next = pending->next;

        if (pending->nestLevel < nestLevel) {
            // Keep outer-level entries for later processing
            prev = pending;
        } else {
            // Remove from list first (unlink before processing)
            if (prev)
                prev->next = next;
            else
                pendingDeletes = next;

            // Process deletion if it matches commit/abort condition
            if (pending->atCommit == isCommit) {
                SMgrRelation srel = smgropen(pending->rlocator, pending->procNumber);

                // Grow relations array as needed
                if (maxrels == 0) {
                    maxrels = 8;
                    srels = palloc(sizeof(SMgrRelation) * maxrels);
                } else if (maxrels <= nrels) {
                    maxrels *= 2;
                    srels = repalloc(srels, sizeof(SMgrRelation) * maxrels);
                }

                srels[nrels++] = srel;
            }

            // Free the processed list entry
            pfree(pending);
        }
    }

    // Batch delete all collected relations
    if (nrels > 0) {
        smgrdounlinkall(srels, nrels, false);

        // Close all opened relations
        for (int i = 0; i < nrels; i++)
            smgrclose(srels[i]);

        pfree(srels);
    }
}
```

Key simplifications made:
- Consolidated variable declarations for better readability
- Added descriptive comments explaining each major section
- Simplified array growth logic while preserving the doubling strategy
- Made the list traversal and unlinking logic clearer
- Emphasized the batch processing approach in comments
- Preserved all essential logic including transaction nesting and commit/abort handling