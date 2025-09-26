# LockRelationForExtension

## Location
[src/backend/storage/lmgr/lmgr.c:420-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L420-L437)

## Overview
Acquires an extension lock on a relation to interlock addition of pages to relations, preventing race conditions when multiple processes attempt to extend a relation simultaneously.

## Definition

```c
void
LockRelationForExtension(Relation relation, LOCKMODE lockmode)
```
## Detailed Description
This function provides locking mechanism for relation extension operations to address race conditions in the buffer manager and storage manager definition of P_NEW (new page allocation). When multiple processes attempt to extend a relation concurrently, this lock ensures that only one process can perform the extension at a time.

The function creates a lock tag specifically for relation extension using SET_LOCKTAG_RELATION_EXTEND macro, which identifies the relation by its database OID and relation OID, then acquires the lock using the standard PostgreSQL locking mechanism. The caller is assumed to already hold some type of regular lock on the relation, so no invalidation message processing is needed.

## Parameters / Member Variables
- `relation`: Pointer to the Relation structure representing the relation to be extended
- `lockmode`: The type of lock to acquire (e.g., ExclusiveLock, ShareLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION_EXTEND (macro to set up lock tag for relation extension)
  - LockAcquire (core lock acquisition function)
- Called from (representative examples):
  - brin_page_cleanup (BRIN index page cleanup)
  - brin_getinsertbuffer (BRIN index buffer management)
  - ginvacuumcleanup (GIN index vacuum cleanup)
  - gistvacuumscan (GiST index vacuum scan)
  - btvacuumscan (B-tree vacuum scan)
  - spgvacuumscan (SP-GiST vacuum scan)
  - ExtendBufferedRelTo (buffered relation extension)
  - ExtendBufferedRelShared (shared buffered relation extension)

## Notes and Other Information
- The lock uses LOCKTAG_RELATION_EXTEND lock tag type which is distinct from regular relation locks
- The function assumes the caller already holds some form of regular lock on the relation
- This mechanism is critical for preventing corruption during concurrent relation extensions
- The lock is typically held for a very short duration during the actual page allocation process
- Used extensively in index maintenance operations and relation extension scenarios