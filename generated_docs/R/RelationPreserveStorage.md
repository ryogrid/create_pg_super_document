# RelationPreserveStorage

## Location
[src/backend/catalog/storage.c:251-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L251-L287)

## Overview
RelationPreserveStorage removes a relation from the pending deletion list, preserving its storage files when they would otherwise be scheduled for deletion.

## Definition
```c
void RelationPreserveStorage(RelFileLocator rlocator, bool atCommit)
```

## Detailed Description
RelationPreserveStorage marks a relation as not to be deleted after all, removing it from the pending deletion list. This function is essential for handling cases where relation mapping changes are committed separately from the main transaction, preventing the deletion of newly installed physical relations even if the transaction aborts. The relation mapper uses this function during its commit phase to preserve relations that have been successfully mapped. The function is also used during ALTER TABLE operations to reuse existing index builds by removing delete-at-commit entries. It searches through the pendingDeletes list for entries matching both the RelFileLocator and the atCommit flag, removing and freeing any matches found.

## Parameters / Member Variables
- `rlocator`: RelFileLocator structure identifying the specific relation file to preserve, containing tablespace, database, relation OIDs and fork information
- `atCommit`: Boolean flag indicating whether to remove commit-time deletion (true) or abort-time deletion (false) entries

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocatorEquals
  - [pfree](../p/pfree.md)
  - [PendingRelDelete](../P/PendingRelDelete.md)
- Called from (representative examples):
  - [write_relmap_file](../w/write_relmap_file.md)
  - [ATExecAddIndex](../A/ATExecAddIndex.md)

## Notes and Other Information
- Function is a no-op if the relation is not found in the pending deletion list
- Handles linked list manipulation carefully, updating both prev and next pointers correctly
- Can remove multiple matching entries if they exist (though typically only one match is expected)
- Critical for relation mapper correctness when mapping updates are committed separately
- Used during ALTER TABLE index operations to preserve existing index builds for reuse
- Memory allocated for removed PendingRelDelete entries is freed using pfree

## Simplified Source

```c
void RelationPreserveStorage(RelFileLocator rlocator, bool atCommit) {
    PendingRelDelete *pending;
    PendingRelDelete *prev = NULL;

    // Search through pending deletion list
    for (pending = pendingDeletes; pending != NULL; pending = pending->next) {
        // Check if this entry matches the relation and deletion type
        if (RelFileLocatorEquals(rlocator, pending->rlocator) &&
            pending->atCommit == atCommit) {
            // Remove entry from linked list
            if (prev)
                prev->next = pending->next;
            else
                pendingDeletes = pending->next;

            // Free the entry and continue with same prev
            pfree(pending);
        } else {
            // Keep this entry, advance prev pointer
            prev = pending;
        }
    }
}
```