# _bt_spooldestroy

## Location
[src/backend/access/nbtree/nbtsort.c:515-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L515-L524)

## Overview
Cleanly destroys a BTSpool structure and releases all associated resources including the underlying tuplesort state.

## Definition

```c
static void
_bt_spooldestroy(BTSpool *btspool)
```
## Detailed Description
 is a cleanup function that properly deallocates a BTSpool structure used during B-tree index construction. The function performs two essential cleanup operations:

1. **Tuplesort Cleanup**: Calls  to properly terminate the associated tuplesort state, ensuring that all temporary files, memory allocations, and other resources used by the sorting subsystem are properly released.

2. **Structure Deallocation**: Frees the BTSpool structure itself using , returning the memory to PostgreSQL's memory management system.

This function is called during the final cleanup phase of index construction to ensure no memory leaks or resource leaks occur. It's designed to be safe to call even when the spool may not have been fully utilized (such as when a secondary spool for dead tuples turns out to be unnecessary).

## Parameters
- : Pointer to the BTSpool structure to be destroyed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  -  - Terminates the associated tuplesort state
  -  - Deallocates the BTSpool structure memory
  -  - The spool structure type being destroyed
- Called from:
  -  - Main index construction cleanup (called twice for primary and secondary spools)
  -  - Early cleanup of unnecessary secondary spool

## Notes and Other Information
- This is a simple but critical cleanup function that prevents memory leaks during index construction
- Safe to call on any properly initialized BTSpool structure
- Must be called for both primary and secondary spools when they exist
- The function assumes the BTSpool structure and its sortstate field are properly initialized
- Part of the resource management strategy that ensures index construction doesn't leave behind temporary resources

## Simplified Source

```c
static void
_bt_spooldestroy(BTSpool *btspool)
{
    // Clean up tuplesort state and release associated resources
    tuplesort_end(btspool->sortstate);

    // Free the spool structure itself
    pfree(btspool);
}
```