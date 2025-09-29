# heap_prune_satisfies_vacuum

## Location
[src/backend/access/heap/pruneheap.c:917-959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L917-L959)

## Overview
heap_prune_satisfies_vacuum performs specialized visibility checks for heap pruning operations, determining whether tuples are DEAD, RECENTLY_DEAD, or still visible during pruning and vacuum operations.

## Definition
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)

## Detailed Description
This function serves as the visibility determination layer specifically for heap pruning operations, providing more nuanced visibility checking than the standard HeapTupleSatisfiesVacuum functions. It performs a multi-level visibility assessment:

1. **Initial HTSV Check**: Uses HeapTupleSatisfiesVacuumHorizon to get the basic visibility status
2. **VACUUM-Specific Logic**: For VACUUM operations (when cutoffs are provided), ensures tuples with xmax older than OldestXmin are considered DEAD to prevent freezing issues
3. **Global Visibility Test**: Uses the current global visibility state to determine if recently dead tuples can now be considered fully dead

The function is critical for ensuring that pruning decisions are made correctly based on both VACUUM's established cutoff points and the current global transaction visibility state, which may have evolved since the beginning of a long-running VACUUM operation.

## Parameters / Member Variables
- : Pruning state containing visibility test context and vacuum cutoffs
- : The heap tuple being evaluated for visibility
- : Buffer containing the tuple (for hint bit setting during visibility checks)

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleSatisfiesVacuumHorizon](../H/HeapTupleSatisfiesVacuumHorizon.md)
  - TransactionIdIsValid
  - NormalTransactionIdPrecedes
  - [GlobalVisTestIsRemovableXid](../G/GlobalVisTestIsRemovableXid.md)
  - HTSV_Result constants (HEAPTUPLE_RECENTLY_DEAD, HEAPTUPLE_DEAD)
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information
- Static function internal to pruneheap.c
- Handles both on-access pruning (no cutoffs) and VACUUM pruning (with cutoffs)
- Critical for preventing freezing of dead tuples during VACUUM
- Uses global visibility state that may be more current than VACUUM's initial OldestXmin
- Returns HTSV_Result enum values to indicate tuple status
- Part of the broader heap tuple visibility checking subsystem

## Simplified Source

```c
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
    HTSV_Result res;
    TransactionId dead_after;

    // Get basic visibility status from standard check
    res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

    // If not recently dead, return the basic result
    if (res != HEAPTUPLE_RECENTLY_DEAD)
        return res;

    // For VACUUM operations, check if tuple is old enough to be considered DEAD
    if (prstate->cutoffs &&
        TransactionIdIsValid(prstate->cutoffs->OldestXmin) &&
        NormalTransactionIdPrecedes(dead_after, prstate->cutoffs->OldestXmin))
        return HEAPTUPLE_DEAD;

    // Check if global visibility has advanced enough to consider tuple DEAD
    if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
        return HEAPTUPLE_DEAD;

    return res;
}
```