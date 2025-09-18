# btvacuumscan

## Location
[src/backend/access/nbtree/nbtree.c:939-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L939-L1072)

## Overview
Scans the entire B-tree index for VACUUM purposes, identifying deletable tuples, empty pages, and recyclable deleted pages.

## Definition
```c
static void btvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                        IndexBulkDeleteCallback callback, void *callback_state,
                        BTCycleId cycleid)
```

## Detailed Description
This is the core function that performs the actual scanning work for B-tree vacuum operations. It combines multiple tasks in a single pass through the index:

1. **Tuple deletion**: Uses the callback function to identify and delete tuples pointing to dead heap tuples
2. **Page deletion**: Identifies empty pages that can be deleted from the index
3. **Page recycling**: Finds old deleted pages that can be safely recycled and added to the Free Space Map

The function scans all index pages except the metapage in physical order, hoping for read-ahead optimization from the kernel. It handles concurrent page additions by repeatedly checking the relation length and using extension locks to prevent race conditions.

Key implementation details:
- Resets per-scan statistics while preserving per-VACUUM statistics
- Creates a temporary memory context for page deletion operations  
- Initializes pending FSM (Free Space Map) optimization state
- Uses a careful locking protocol to handle concurrent relation extensions
- Updates progress reporting for long-running operations
- Finalizes FSM operations at the end to make deleted pages available for reuse

The function is designed to handle both bulk delete operations (with callback) and cleanup-only operations (callback is NULL).

## Parameters / Member Variables
- `info`: IndexVacuumInfo structure containing vacuum operation details and configuration
- `stats`: IndexBulkDeleteResult structure for accumulating statistics across the scan
- `callback`: IndexBulkDeleteCallback function to determine which tuples should be deleted (NULL for cleanup-only scans)
- `callback_state`: Opaque state data passed to the callback function
- `cycleid`: BTCycleId for coordinating vacuum operations and preventing conflicts

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [_bt_pendingfsm_init](_bt_pendingfsm_init.md)
  - LockRelationForExtension / UnlockRelationForExtension  
  - RelationGetNumberOfBlocks
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [btvacuumpage](btvacuumpage.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [_bt_pendingfsm_finalize](_bt_pendingfsm_finalize.md)
  - IndexFreeSpaceMapVacuum
- Types used:
  - [IndexVacuumInfo](../I/IndexVacuumInfo.md)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)
  - IndexBulkDeleteCallback
  - BTCycleId
  - [BTVacState](../B/BTVacState.md)
  - BlockNumber
  - [Relation](../R/Relation.md)
- Constants used:
  - BTREE_METAPAGE
  - ALLOCSET_DEFAULT_SIZES
  - PROGRESS_SCAN_BLOCKS_TOTAL
  - PROGRESS_SCAN_BLOCKS_DONE
  - ExclusiveLock
- Macros used:
  - RELATION_IS_LOCAL
- Called from:
  - [btbulkdelete](btbulkdelete.md)
  - [btvacuumcleanup](btvacuumcleanup.md)

## Notes and Other Information
- This is a static function, only accessible within the nbtree.c file
- Handles both deletion and cleanup-only scans through the same code path
- Uses extension locks to prevent race conditions with concurrent page additions, though this may no longer be necessary with newer page locking mechanisms
- The scan must visit all leaf pages, including those added during the scan, to ensure completeness
- Memory management includes a temporary context for page deletion operations to prevent memory leaks
- Progress reporting is integrated for operations that may take significant time
- FSM (Free Space Map) operations are optimized and batched for efficiency
- The function carefully manages statistics to avoid double-counting in multi-scan VACUUM operations