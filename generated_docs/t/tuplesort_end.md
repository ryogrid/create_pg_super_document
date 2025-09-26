# tuplesort_end

## Location
[src/backend/utils/sort/tuplesort.c:971-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L971-L987)

## Overview
Public function that completely terminates a tuplesort operation by releasing all resources and destroying the main memory context.

## Definition
```c
void tuplesort_end(Tuplesortstate *state)
```

## Detailed Description
This function provides the public interface for completely terminating a tuplesort operation. It performs a two-stage cleanup process: first calling tuplesort_free() to handle sort-specific resource cleanup, then deleting the main memory context which includes the Tuplesortstate struct itself.

After calling this function, the tuplesort state becomes completely invalid and any pointers previously returned by tuplesort_getXXX functions point to deallocated memory. This is the final cleanup step in the tuplesort lifecycle and must be called to prevent memory leaks.

The function ensures complete cleanup of both working memory (handled by tuplesort_free) and the main sort context including the state structure itself.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure to be terminated and freed

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_free (internal resource cleanup function)
  - MemoryContextDelete (PostgreSQL memory context deletion)

- Called from (representative examples):
  - _brin_parallel_merge (BRIN index parallel merge completion)
  - gistbuild (GiST index build completion) 
  - _h_spooldestroy (hash index spool cleanup)
  - _bt_spooldestroy (B-tree index spool cleanup)
  - ExecEndAgg (aggregate node cleanup)
  - ExecEndSort (sort node cleanup)
  - ExecEndIncrementalSort (incremental sort node cleanup)
  - ordered_set_shutdown (ordered set aggregate cleanup)

## Notes and Other Information
- This is the final function to call in the tuplesort lifecycle
- After calling this function, the state pointer becomes invalid and should not be used
- All pointers returned by previous tuplesort_getXXX calls become invalid garbage pointers
- Must be called to prevent memory leaks - failure to call will leave the main memory context allocated
- Used extensively throughout PostgreSQL for cleanup in index builds, executor nodes, and aggregates
- The function is designed to be safe to call once per tuplesort state
- Memory context deletion ensures complete cleanup even if some resources were missed by tuplesort_free