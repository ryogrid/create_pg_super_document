# gistBuildCallback

## Location
[src/backend/access/gist/gistbuild.c:820-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L820-L906)

## Overview
Per-tuple callback function used during GiST index construction that processes each heap tuple, converts it to an index tuple, and inserts it into the index using the appropriate insertion method.

## Definition

```c
static void
gistBuildCallback(Relation index,
				  ItemPointer tid,
				  Datum *values,
				  bool *isnull,
				  bool tupleIsAlive,
				  void *state)
```
## Detailed Description
This function serves as the callback for  during GiST index construction. It processes each tuple from the heap table and handles the complex logic of building the index efficiently.

Key responsibilities include:
1. **Index Tuple Formation**: Converts heap tuple data into a properly formatted index tuple using 
2. **Statistics Tracking**: Maintains counters for tuple count and total size for buffer sizing calculations
3. **Insertion Method Selection**: Routes tuples to either buffered insertion () or direct insertion () based on current build mode
4. **Dynamic Mode Switching**: Monitors index growth and automatically switches to buffering mode when the index becomes too large to fit in cache
5. **Buffer Size Adjustment**: Periodically recalculates optimal buffer sizes based on accumulated tuple size statistics
6. **Memory Management**: Properly manages memory contexts to prevent leaks during long index builds

The function implements adaptive behavior, starting with direct insertion and potentially switching to buffering mode when beneficial. It supports multiple buffering modes: AUTO (switches based on cache size), STATS (switches after collecting tuple statistics), and ACTIVE (buffering already enabled).

## Parameters / Member Variables
- : The index relation being built
- : ItemPointer to the heap tuple being indexed
- : Array of Datum values extracted from the heap tuple
- : Array indicating which values are NULL
- : Boolean indicating if the tuple is alive (used for concurrent builds)
- : Pointer to GISTBuildState structure containing build context and statistics

## Dependencies
- Functions called/Symbols referenced:
  - [gistFormTuple](gistFormTuple.md)
  - [gistBufferingBuildInsert](gistBufferingBuildInsert.md)
  - [gistdoinsert](gistdoinsert.md)
  - [calculatePagesPerBuffer](../c/calculatePagesPerBuffer.md)
  - [gistInitBuffering](gistInitBuffering.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - IndexTupleSize
  - [smgrnblocks](../s/smgrnblocks.md)
  - [RelationGetSmgr](../R/RelationGetSmgr.md)
- Called from (representative examples):
  - [gistbuild](gistbuild.md) (via table_index_build_scan)

## Notes and Other Information
- Contains a known memory management issue (XXX comment) where tempCxt is reset in multiple locations, which could potentially lead to dangling pointers
- Uses constants like BUFFERING_MODE_TUPLE_SIZE_STATS_TARGET and BUFFERING_MODE_SWITCH_CHECK_STEP to control when buffer adjustments and mode switches occur
- The function is performance-critical as it's called once for every tuple being indexed
- Implements a feedback loop where buffer sizes are continuously optimized based on observed tuple characteristics
- Mode switching logic prevents excessive calls to expensive operations like  by checking conditions only periodically