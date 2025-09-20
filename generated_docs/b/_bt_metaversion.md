# _bt_metaversion

## Location
[src/backend/access/nbtree/nbtpage.c:739-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L739-L796)

## Overview
_bt_metaversion extracts version and capability information from the B-tree metadata page to determine index behavior characteristics and supported features.

## Definition

```c
void
_bt_metaversion(Relation rel, bool *heapkeyspace, bool *allequalimage)
```
## Detailed Description
This function retrieves critical version and feature information from the B-tree metapage to determine how the index should behave during operations. Key aspects:

1. **Version Detection**: Determines if the index uses "heapkeyspace" semantics (version 4+) where heap TID is treated as a tiebreaker attribute for duplicate keys.

2. **Deduplication Safety**: Checks the allequalimage flag to determine whether deduplication optimizations can be safely applied to the index.

3. **Upgrade Compatibility**: Handles differences between pg_upgrade'd version 3 indexes and native version 4+ indexes, ensuring proper performance characteristics are maintained.

4. **Insertion Scankey Support**: Provides information needed to construct proper BTScanInsert structures for index operations.

5. **Metadata Caching**: Uses cached metadata when available to avoid repeated reads, with special handling for version upgrades.

The function is essential for ensuring that index operations use the correct algorithms based on the index version and capabilities.

## Parameters / Member Variables
- : The B-tree index relation to examine
- : Output parameter set to true if index uses heap TID as tiebreaker (version > BTREE_NOVAC_VERSION)
- : Output parameter indicating whether deduplication is safe for this index

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md): Acquires buffer for metadata page
  - _bt_getmeta: Gets metadata from metapage  
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocates memory for caching metadata
  - [BTMetaPageData](../B/BTMetaPageData.md): Metadata structure type
  - BTREE_NOVAC_VERSION: Version constant for heapkeyspace transition
- Called from (representative examples):
  - [_bt_first](_bt_first.md): Beginning of index scans
  - [_bt_mkscankey](_bt_mkscankey.md): Creating scan keys for index operations

## Notes and Other Information
- Critical for determining proper index traversal and insertion algorithms
- Handles version transitions gracefully, including on-the-fly upgrades
- The heapkeyspace feature (version 4+) significantly affects duplicate key handling
- allequalimage flag is zero for pg_upgrade'd indexes from Postgres 12 for safety
- Empty indexes (no root) still provide version information for proper operation
- Caching strategy balances performance with correctness across version upgrades
- The function is located in src/backend/access/nbtree/nbtpage.c:739-796