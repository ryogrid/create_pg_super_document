# heap_prune_chain

## Location
src/backend/access/heap/pruneheap.c: 999 - 1200

## Overview
heap_prune_chain processes a HOT (Heap-Only Tuple) chain by determining the fate of each tuple in the chain and planning the appropriate pruning actions based on tuple visibility status.

## Definition
static void heap_prune_chain(Page page, BlockNumber blockno, OffsetNumber maxoff, OffsetNumber rootoffnum, PruneState *prstate)

## Detailed Description
This function implements the core logic for processing HOT chains during heap pruning operations. It traverses the entire chain starting from a root line pointer and determines the appropriate action for each tuple based on its visibility status. The function operates in several phases:

**Chain Traversal**: Follows the chain from root to end, validating each link by checking XMIN against the previous tuple's XMAX and ensuring HOT update relationships are maintained.

**Visibility-Based Processing**: For each tuple in the chain, uses cached HTSV results to determine if tuples are DEAD, RECENTLY_DEAD, or still live. DEAD tuples are candidates for removal, and RECENTLY_DEAD tuples preceding DEAD tuples are also considered removable.

**Pruning Strategy**: Implements three main strategies:
1. **No Dead Tuples**: Leave the entire chain unchanged
2. **Entire Chain Dead**: Mark root as LP_DEAD and remove all other tuples  
3. **Partial Chain Dead**: Redirect root to first live tuple and remove dead predecessors

**Planning Phase**: Records planned changes in prstate arrays (redirected, nowdead, nowunused) rather than modifying the page directly. This allows the changes to be applied atomically later in a critical section.

The function ensures that no DEAD tuples with storage remain after pruning, as VACUUM cannot handle such cases.

## Parameters / Member Variables
- : The heap page containing the HOT chain
- : Block number of the page (for validation)
- : Maximum offset number on the page
- : Starting offset number of the HOT chain root
- : Pruning state containing visibility cache and change tracking arrays

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md), PageGetItem
  - ItemId manipulation functions (ItemIdIsRedirected, ItemIdIsNormal, etc.)
  - HeapTupleHeader functions (HeapTupleHeaderGetXmin, HeapTupleHeaderIsHotUpdated, etc.)
  - [htsv_get_valid_status](htsv_get_valid_status.md) (for accessing cached visibility)
  - [HeapTupleHeaderAdvanceConflictHorizon](../H/HeapTupleHeaderAdvanceConflictHorizon.md)
  - heap_prune_record_* functions (for recording planned changes)
  - ItemPointer functions for following chain links
- Called from (representative examples):
  - [heap_page_prune_and_freeze](heap_page_prune_and_freeze.md)

## Notes and Other Information  
- Static function internal to pruneheap.c
- Handles complex cases like broken redirect chains and partition movement validation
- Maintains conflict horizons for hot standby safety during tuple removal
- Uses cached HTSV results to avoid recomputing visibility
- Implements the "RECENTLY_DEAD preceding DEAD is also DEAD" optimization
- Critical for HOT optimization correctness - ensures chain integrity is maintained
- Defensive programming includes extensive assertions and error checking
- Part of the two-phase pruning approach (plan then execute)