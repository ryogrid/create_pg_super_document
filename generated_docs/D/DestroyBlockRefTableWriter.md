# DestroyBlockRefTableWriter

## Location
[src/common/blkreftable.c:855-874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L855-L874)

## Overview
DestroyBlockRefTableWriter finalizes the writing process of a block reference table file and releases the resources associated with the BlockRefTableWriter.

## Definition
```c
void DestroyBlockRefTableWriter(BlockRefTableWriter *writer)
```

## Detailed Description
This function completes the incremental writing process of a block reference table file by performing final termination operations on the writer's buffer and then freeing the memory allocated for the writer structure. The function ensures proper finalization of the output file by calling BlockRefTableFileTerminate, which handles final buffer flushing, CRC writing, and other cleanup operations before deallocating the writer.

This is typically the final step in the incremental block reference table writing workflow, after all entries have been written using BlockRefTableWriteEntry.

## Parameters / Member Variables
- `writer`: The BlockRefTableWriter instance to be finalized and destroyed

## Dependencies
- Functions called/Symbols referenced:
  - BlockRefTableFileTerminate (finalizes the buffer and output file)
  - [pfree](../p/pfree.md) (frees the allocated memory)
  - BlockRefTableWriter (writer structure type)

- Called from (representative examples):
  - Functions that complete block reference table generation
  - Cleanup code in backup utilities
  - Error handling paths that need to clean up partial writes

## Notes and Other Information
- This function should be called exactly once for each BlockRefTableWriter created
- The function handles proper finalization of the output file including CRC computation
- Memory deallocation is performed using pfree, matching the palloc0 allocation in CreateBlockRefTableWriter
- After calling this function, the writer pointer becomes invalid and should not be used
- This completes the lifecycle of a BlockRefTableWriter: Create → WriteEntry(s) → Destroy
- Proper resource cleanup is essential to avoid memory leaks and ensure complete file writing