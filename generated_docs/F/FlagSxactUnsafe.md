# FlagSxactUnsafe

## Location
[src/backend/storage/lmgr/predicate.c:699-730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L699-L730)

## Overview
Marks a read-only serializable transaction as unsafe and cleans up its possible unsafe conflict records.

## Definition

```c
static void
FlagSxactUnsafe(SERIALIZABLEXACT *sxact)
```
## Detailed Description
This function flags a read-only serializable transaction as unsafe by setting the SXACT_FLAG_RO_UNSAFE flag and then cleans up all possible unsafe conflicts associated with the transaction. Once a read-only transaction is determined to be unsafe (meaning it cannot use a safe snapshot), there's no need to track potential conflicts anymore, so all records in the possibleUnsafeConflicts list are released back to the pool.

## Parameters / Member Variables
- : Pointer to the read-only serializable transaction to be flagged as unsafe

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsReadOnly
  - SxactIsROSafe
  - dlist_foreach_modify
  - dlist_container
  - [ReleaseRWConflict](../R/ReleaseRWConflict.md)
- Types referenced:
  - [SERIALIZABLEXACT](../S/SERIALIZABLEXACT.md)
  - [dlist_mutable_iter](../d/dlist_mutable_iter.md)
  - [RWConflict](../R/RWConflict.md)
  - [RWConflictData](../R/RWConflictData.md)
- Constants referenced:
  - SXACT_FLAG_RO_UNSAFE
- Called from (representative examples):
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md)

## Notes and Other Information
- Asserts that the transaction is read-only and not already flagged as safe
- Sets the SXACT_FLAG_RO_UNSAFE flag to mark the transaction as unsafe
- Iterates through possibleUnsafeConflicts using dlist_foreach_modify for safe deletion
- Releases all conflict records back to the RWConflictPool
- Part of PostgreSQL's safe snapshot optimization for read-only transactions
- Located in src/backend/storage/lmgr/predicate.c:699-730

## Simplified Source

```c
// Simplified version of FlagSxactUnsafe
static void FlagSxactUnsafe(SERIALIZABLEXACT *sxact) {
    dlist_mutable_iter iter;

    // Mark the read-only transaction as unsafe
    sxact->flags |= SXACT_FLAG_RO_UNSAFE;

    // Clean up all possible unsafe conflict records
    // Once unsafe, we don't need to track potential conflicts anymore
    dlist_foreach_modify(iter, &sxact->possibleUnsafeConflicts) {
        RWConflict conflict =
            dlist_container(RWConflictData, inLink, iter.cur);

        // Release this conflict back to the pool
        ReleaseRWConflict(conflict);
    }
}
```

Key simplifications made:
- Removed debug assertions for clarity
- Added clear comments explaining the purpose
- Simplified the conflict cleanup logic description
- Focused on the core functionality: flag setting and conflict cleanup