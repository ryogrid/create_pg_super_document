# PredicateLockPage

## Location
src/backend/storage/lmgr/predicate.c: 2589 - 2610

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
  - SerializationNeededForRead
  - SET_PREDICATELOCKTARGETTAG_PAGE
  - PredicateLockAcquire
  - PREDICATELOCKTARGETTAG (struct)
  - BlockNumber (type)
- Called from (representative examples):
  - gistScanPage
  - _bt_first, _bt_readnextpage, _bt_endpoint (B-tree operations)
  - _hash_first, _hash_readnext (hash index operations)
  - GIN index operations (moveRightIfItNeeded, collectMatchBitmap, etc.)
  - IndexOnlyNext

## Notes and Other Information
- Provides intermediate granularity between relation-level and tuple-level predicate locks
- Commonly used during index scans and page-oriented operations across multiple access methods
- Will be skipped if a coarser relation-level lock already covers the page
- Automatically cleans up any tuple-level locks that may exist on the same page
- Extensively used by various index access methods (B-tree, Hash, GIN, GiST) during page scanning
- Critical for maintaining serializable isolation while allowing reasonable concurrency in page-based operations
- The block number parameter allows precise targeting of specific pages within large relations