# heap_prune_record_unused

## Location
[src/backend/access/heap/pruneheap.c:1297-1318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1297-L1318)

## Overview
Records a line pointer that should be marked as unused (LP_UNUSED) during heap pruning, enabling immediate reclamation of space occupied by dead tuples.

## Definition

```c
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum, bool was_normal)
```
## Detailed Description
This function records that a line pointer at the specified offset should be marked as unused (LP_UNUSED) during the heap pruning operation. Unlike dead line pointers which require a later VACUUM to reclaim space, unused line pointers immediately free up their space for reuse by new tuples. This provides more aggressive space reclamation during pruning operations when conditions allow for immediate cleanup.

The function tracks unused line pointers in the pruning state's `nowunused` array and maintains statistics about the pruning operation.

## Parameters / Member Variables
- `prstate`: Pointer to the PruneState structure tracking the current pruning operation
- `offnum`: The offset number of the line pointer to be marked unused
- `was_normal`: Boolean indicating whether the original line pointer pointed to a normal tuple (as opposed to a redirect)

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure)
  - MaxHeapTuplesPerPage (constant)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)
  - [heap_prune_chain](heap_prune_chain.md)
  - [heap_prune_record_dead_or_unused](heap_prune_record_dead_or_unused.md)

## Notes and Other Information
- Marks the offset as processed to prevent duplicate processing
- Maintains the `nowunused` array for tracking line pointers to be marked LP_UNUSED
- Only counts deletions when marking a normal tuple as unused (not redirects)
- Provides more aggressive space reclamation compared to marking as dead
- Used when immediate space reclamation is possible and beneficial
- Part of PostgreSQL's heap pruning optimization for better space utilization
- Allows new tuples to reuse the space immediately without waiting for VACUUM

## Simplified Source

```c
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum, bool was_normal)
{
    // Mark as processed to avoid duplicate handling
    prstate->processed[offnum] = true;

    // Add to list of line pointers to mark as unused
    prstate->nowunused[prstate->nunused] = offnum;
    prstate->nunused++;

    // Count as deletion if it was a normal tuple (not a redirect)
    if (was_normal)
        prstate->ndeleted++;
}
```