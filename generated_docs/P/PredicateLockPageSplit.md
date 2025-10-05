# PredicateLockPageSplit

## Location
[src/backend/storage/lmgr/predicate.c:3134-3218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3134-L3218)

## Overview
Handles the transfer of predicate locks from an old page to a new page during page splits in PostgreSQL's serializable snapshot isolation implementation.

## Definition

```c
void
PredicateLockPageSplit(Relation relation, BlockNumber oldblkno,
					   BlockNumber newblkno)
```
## Detailed Description
PredicateLockPageSplit is a critical function in PostgreSQL's predicate locking system that maintains serializable isolation guarantees during page split operations. When a page is split (due to overflow or other reasons), any existing predicate locks on the old page must be copied to the new page to ensure that serializable transactions continue to detect potential conflicts correctly.

The function performs several key operations:
1. Early bailout if no serializable transactions are running
2. Verification that predicate locking is needed for the relation
3. Creation of predicate lock target tags for both old and new pages  
4. Atomic transfer of locks from old page to new page under exclusive lock
5. Fallback to relation-level locks if page-level lock entries are exhausted

The function handles the case where the predicate lock table becomes full by promoting page locks to relation locks, ensuring that serialization conflicts are still detected even when resources are constrained.

## Parameters / Member Variables
- `relation`: The relation containing the pages being split
- `oldblkno`: Block number of the original page before the split
- `newblkno`: Block number of the newly created page after the split
## Dependencies
- Functions called/Symbols referenced:
  - [PredicateLockingNeededForRelation](PredicateLockingNeededForRelation.md)
  - BlockNumberIsValid
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - [TransferPredicateLocksToNewTarget](../T/TransferPredicateLocksToNewTarget.md)
  - [GetParentPredicateLockTag](../G/GetParentPredicateLockTag.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
- Called from (representative examples):
  - [ginPlaceToPage](../g/ginPlaceToPage.md) (GIN index splits)
  - [gistplacetopage](../g/gistplacetopage.md) (GiST index splits)
  - [_hash_splitbucket](../h/_hash_splitbucket.md) (hash index splits)
  - [_bt_insertonpg](../b/_bt_insertonpg.md) (B-tree index splits)

## Notes and Other Information
- This function affects ALL serializable transactions, regardless of the isolation level of the transaction performing the page split
- The function may leave local lock copies inconsistent with shared memory, but this is acceptable since the new locks provide equivalent or stronger conflict detection
- Memory barriers from LWLock acquisition ensure safe concurrent access during the serializable transaction check
- Skip processing for temporary tables and toast tables as they don't require predicate locking
- The function is essential for maintaining the correctness of PostgreSQL's serializable snapshot isolation implementation

## Simplified Source

```c
void PredicateLockPageSplit(Relation relation, BlockNumber oldblkno,
                           BlockNumber newblkno)
{
    PREDICATELOCKTARGETTAG oldtargettag;
    PREDICATELOCKTARGETTAG newtargettag;
    bool success;

    // Quick exit if no serializable transactions running
    if (!TransactionIdIsValid(PredXact->SxactGlobalXmin))
        return;

    if (!PredicateLockingNeededForRelation(relation))
        return;

    Assert(oldblkno != newblkno);
    Assert(BlockNumberIsValid(oldblkno));
    Assert(BlockNumberIsValid(newblkno));

    // Set up target tags for old and new pages
    SET_PREDICATELOCKTARGETTAG_PAGE(oldtargettag,
                                    relation->rd_locator.dbOid,
                                    relation->rd_id,
                                    oldblkno);
    SET_PREDICATELOCKTARGETTAG_PAGE(newtargettag,
                                    relation->rd_locator.dbOid,
                                    relation->rd_id,
                                    newblkno);

    LWLockAcquire(SerializablePredicateListLock, LW_EXCLUSIVE);

    // Try copying locks to new page
    success = TransferPredicateLocksToNewTarget(oldtargettag,
                                                newtargettag,
                                                false);

    if (!success)
    {
        // Out of memory - promote to relation lock instead
        success = GetParentPredicateLockTag(&oldtargettag, &newtargettag);
        Assert(success);

        // Move locks to relation level (this should always succeed)
        success = TransferPredicateLocksToNewTarget(oldtargettag,
                                                    newtargettag,
                                                    true);
        Assert(success);
    }

    LWLockRelease(SerializablePredicateListLock);
}
```