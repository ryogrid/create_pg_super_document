# heap_page_prune_execute

## Location
src/backend/access/heap/pruneheap.c: 1561 - 1736

## Overview
Performs the actual physical modifications to a heap page during pruning, including redirecting line pointers, marking items as dead or unused, and repairing page fragmentation.

## Definition
```c
void heap_page_prune_execute(Buffer buffer, bool lp_truncate_only,
                            OffsetNumber *redirected, int nredirected,
                            OffsetNumber *nowdead, int ndead,
                            OffsetNumber *nowunused, int nunused)
```

## Detailed Description
This function is the execution phase of heap page pruning that applies the actual changes to the page structure. It operates in two modes:

1. **Full Pruning Mode** (`lp_truncate_only = false`): Performs complete pruning including redirections, dead marking, unused marking, and page defragmentation. Requires a cleanup lock.

2. **Truncate-Only Mode** (`lp_truncate_only = true`): Only converts LP_DEAD line pointers to LP_UNUSED and truncates the line pointer array. Requires only an exclusive lock.

The function performs three main types of operations:
- **Redirections**: Updates line pointers to redirect to new locations (for HOT chain management)
- **Dead Marking**: Marks line pointers as LP_DEAD when they can't be immediately removed due to potential index references
- **Unused Marking**: Marks line pointers as LP_UNUSED when they can be safely reclaimed

After applying changes, it repairs page fragmentation and validates redirect integrity.

## Parameters / Member Variables
- `buffer`: Buffer containing the heap page to be modified
- `lp_truncate_only`: If true, only perform LP_DEAD to LP_UNUSED conversion and truncation
- `redirected`: Array of pairs (from_offset, to_offset) for line pointer redirections
- `nredirected`: Number of redirection pairs in the redirected array
- `nowdead`: Array of offset numbers to be marked as LP_DEAD
- `ndead`: Number of offsets in the nowdead array
- `nowunused`: Array of offset numbers to be marked as LP_UNUSED
- `nunused`: Number of offsets in the nowunused array

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), PageGetItemId, PageGetItem (page access functions)
  - ItemIdSetRedirect, ItemIdSetDead, ItemIdSetUnused (line pointer modification)
  - ItemIdIsRedirected, ItemIdHasStorage, ItemIdIsNormal, ItemIdIsDead, ItemIdIsUsed (line pointer state checks)
  - HeapTupleHeaderIsHeapOnly (tuple header checks)
  - [PageRepairFragmentation](../P/PageRepairFragmentation.md), PageTruncateLinePointerArray (page maintenance)
  - [page_verify_redirects](../p/page_verify_redirects.md) (validation)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)
  - [heap_xlog_prune_freeze](heap_xlog_prune_freeze.md) (during WAL replay)

## Notes and Other Information
- This is a public function (not static) as it's used by WAL replay mechanisms
- Contains extensive assertion checking to validate HOT chain integrity and line pointer states
- The function assumes that HOT (Heap-Only Tuple) invariants are maintained throughout the process
- Critical for maintaining index consistency by ensuring LP_REDIRECT items exist for index TID references
- The two-mode operation allows for different locking requirements depending on the extent of changes needed
- Part of PostgreSQL's space reclamation and HOT update optimization system