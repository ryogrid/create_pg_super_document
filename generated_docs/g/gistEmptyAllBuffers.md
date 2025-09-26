# gistEmptyAllBuffers

## Location
[src/backend/access/gist/gistbuild.c:1370-1424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L1370-L1424)

## Overview
Empties all node buffers from top to bottom levels at the end of GiST index construction to flush all remaining tuples to the index.

## Definition

```c
static void
gistEmptyAllBuffers(GISTBuildState *buildstate)
```
## Detailed Description
This function performs the final cleanup phase of GiST buffering-based index construction by systematically emptying all remaining buffers from the highest level down to the lowest level. It processes each level completely before moving to the next lower level, ensuring that all buffered tuples are properly inserted into the index.

The function iterates through levels in descending order and processes all buffers at each level. For each non-empty buffer, it adds the buffer to the emptying queue and calls gistProcessEmptyingQueue() to handle the actual emptying. The function handles dynamic changes to the buffer lists that can occur during processing due to page splits.

Once a buffer is completely emptied (blocksCount == 0), it is removed from the level's buffer list. This process continues until all buffers at all levels have been processed and emptied.

## Parameters / Member Variables
- : GiST build state containing the buffer management structures and build context

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - linitial
  - [lcons](../l/lcons.md)
  - [gistProcessEmptyingQueue](gistProcessEmptyingQueue.md)
  - [list_delete_first](../l/list_delete_first.md)
  - elog
- Called from (representative examples):
  - [gistbuild](gistbuild.md)

## Notes and Other Information
- Called at the end of index build to ensure all buffered tuples are flushed to the index
- Destroys the buffersOnLevels lists as a side effect, so no further buffer insertions should occur after this call
- Processes levels from top to bottom to maintain proper insertion order and dependencies
- Handles dynamic buffer list changes during processing due to page splits
- Uses temporary memory context switching to manage memory efficiently during the emptying process
- Logs debug information about the completion of each level's buffer emptying
- Critical for ensuring index build completion and data integrity