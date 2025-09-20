# _hash_vacuum_one_page

## Location
[src/backend/access/hash/hashinsert.c:370-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L370-L465)

## Overview
The  function removes dead tuples (LP_DEAD items) from a single hash index page to reclaim space, used opportunistically during insertions when cleanup locks are available.

## Definition

```c
static void
_hash_vacuum_one_page(Relation rel, Relation hrel, Buffer metabuf, Buffer buf)
```
## Detailed Description
This function performs localized vacuum operations on a single hash index page by removing tuples marked as LP_DEAD. It is called opportunistically during insertion operations when:

1. A page lacks sufficient space for a new tuple
2. The page contains dead tuples (indicated by LH_PAGE_HAS_DEAD_TUPLES flag)
3. A cleanup lock is available on the page

The vacuum process involves:

1. **Dead tuple identification**: Scanning all tuples on the page to identify those marked with LP_DEAD status
2. **Conflict horizon calculation**: Computing the snapshot conflict horizon for logical decoding to ensure proper handling on standby servers
3. **Bulk deletion**: Removing all identified dead tuples in a single operation
4. **Metadata updates**: 
   - Clearing the LH_PAGE_HAS_DEAD_TUPLES flag on the page
   - Decrementing the global tuple count in the metapage
5. **WAL logging**: Recording the vacuum operation for crash recovery and replication

The function is designed to be lightweight and non-blocking, only performing cleanup when conditions are favorable.

## Parameters / Member Variables
- : The hash index relation being vacuumed
- : The heap relation (used for snapshot conflict horizon calculation)
- : Buffer containing the hash index metapage (for tuple count updates)
- : Buffer containing the page to be vacuumed (must have cleanup lock)

## Dependencies
- Functions called/Symbols referenced:
  - , : Page inspection functions
  - : Check if tuple is marked as dead
  - : Calculate snapshot conflict horizon
  - : Bulk delete dead tuples
  - , : Access hash-specific page structures
  - : Check if WAL logging is required
  - , , : WAL logging functions
  - Various buffer management functions for locking and marking dirty

- Called from (representative examples):
  - : During tuple insertion when space is needed

## Notes and Other Information
- This is a static function, only callable within the same source file
- Requires a cleanup lock on the target page before being called
- The function is atomic - either all dead tuples are removed or none
- Updates the global tuple count in the metapage, requiring exclusive lock on metabuf
- The LH_PAGE_HAS_DEAD_TUPLES flag clearing is optimistic - there might be newly dead tuples not included in this cleanup
- WAL logging includes both the vacuum operation details and the array of deleted offset numbers for standby server processing
- Uses critical sections to ensure atomicity of the cleanup operation
- The snapshot conflict horizon is important for logical replication to handle conflicts properly on standby servers
- This function provides an efficient way to reclaim space without requiring a full vacuum operation