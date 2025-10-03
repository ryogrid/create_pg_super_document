# PredicateLockPage

## Location
[src/backend/storage/lmgr/predicate.c:2589-2610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2589-L2610)

## Overview
Acquires a predicate lock at the page level for serializable transactions, providing medium-grained locking for specific database pages.

## Definition
void PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot)

## Detailed Description
This function provides the public interface for acquiring page-level predicate locks in PostgreSQL's serializable snapshot isolation implementation. It represents a middle tier in the predicate locking granularity hierarchy, offering more concurrency than relation-level locks while using fewer resources than tuple-level locks.

The function follows the same pattern as PredicateLockRelation but targets a specific page within a relation. It first validates that serialization is needed using SerializationNeededForRead, then constructs a PREDICATELOCKTARGETTAG that includes the database OID, relation OID, and specific block number. The actual lock acquisition is handled by PredicateLockAcquire, which will check for covering coarser locks and potentially promote or clean up finer-grained locks as appropriate.

## Parameters / Member Variables
- : Pointer to the Relation structure representing the table containing the page
- : Block number (BlockNumber) identifying the specific page within the relation to be locked  
- : Pointer to the Snapshot being used for the current operation

## Dependencies
- Functions called/Symbols referenced:
  - [SerializationNeededForRead](../S/SerializationNeededForRead.md)
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - [PredicateLockAcquire](PredicateLockAcquire.md)
  - [PREDICATELOCKTARGETTAG](PREDICATELOCKTARGETTAG.md) (struct)
  - BlockNumber (type)
- Called from (representative examples):
  - [gistScanPage](../g/gistScanPage.md)
  - [_bt_first](../b/_bt_first.md), _bt_readnextpage, _bt_endpoint (B-tree operations)
  - [_hash_first](../h/_hash_first.md), _hash_readnext (hash index operations)
  - GIN index operations (moveRightIfItNeeded, collectMatchBitmap, etc.)
  - [IndexOnlyNext](../I/IndexOnlyNext.md)

## Notes and Other Information
- Provides intermediate granularity between relation-level and tuple-level predicate locks
- Commonly used during index scans and page-oriented operations across multiple access methods
- Will be skipped if a coarser relation-level lock already covers the page
- Automatically cleans up any tuple-level locks that may exist on the same page
- Extensively used by various index access methods (B-tree, Hash, GIN, GiST) during page scanning
- Critical for maintaining serializable isolation while allowing reasonable concurrency in page-based operations
- The block number parameter allows precise targeting of specific pages within large relations

## Simplified Source

```c
void
PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot)
{
    PREDICATELOCKTARGETTAG tag;

    // Check if serialization is needed for this read operation
    if (!SerializationNeededForRead(relation, snapshot))
        return;

    // Set up the predicate lock target tag for the specific page
    SET_PREDICATELOCKTARGETTAG_PAGE(tag,
                                   relation->rd_locator.dbOid,
                                   relation->rd_id,
                                   blkno);

    // Acquire the predicate lock on the page
    PredicateLockAcquire(&tag);
}
```