# BumpContext

## Location
[src/backend/utils/mmgr/bump.c:66-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L66-L78)

## Overview
BumpContext is a memory context structure that implements a bump pointer allocation strategy, designed for efficient allocation of temporary memory with minimal overhead and fast reset capabilities.

## Definition

```c
typedef struct BumpContext
{
	MemoryContextData header;	/* Standard memory-context fields */

	/* Bump context parameters */
	uint32		initBlockSize;	/* initial block size */
	uint32		maxBlockSize;	/* maximum block size */
	uint32		nextBlockSize;	/* next block size to allocate */
	uint32		allocChunkLimit;	/* effective chunk size limit */

	dlist_head	blocks;			/* list of blocks with the block currently
								 * being filled at the head */
} BumpContext;
```
## Detailed Description
BumpContext represents a memory context that uses the bump pointer allocation strategy, where memory is allocated sequentially from large pre-allocated blocks. This allocation strategy is extremely fast for allocation (just advancing a pointer) and allows for very efficient bulk deallocation by resetting the entire context. The context maintains a list of memory blocks and tracks allocation parameters that control block sizing behavior.

The bump allocator is particularly well-suited for scenarios where:
- Many small to medium-sized allocations are needed
- Memory is allocated frequently but freed infrequently (bulk reset)
- Allocation speed is critical
- Memory fragmentation is not a concern (since memory is never individually freed)

## Parameters / Member Variables
- `header`: Standard MemoryContextData structure containing common memory context fields
- `initBlockSize`: The initial size for the first block allocated by this context
- `maxBlockSize`: The maximum size that any block in this context can grow to
- `nextBlockSize`: The size that will be used for the next block allocation (grows over time)
- `allocChunkLimit`: The effective limit for chunk sizes that can be allocated from regular blocks
- `blocks`: Doubly-linked list of BumpBlock structures, with the currently active block at the head
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextData](../M/MemoryContextData.md) (inherited structure)
  - [dlist_head](../d/dlist_head.md) (for block list management)
- Called from (representative examples):
  - [BumpContextCreate](BumpContextCreate.md)
  - [BumpReset](BumpReset.md)
  - [BumpAlloc](BumpAlloc.md)
  - [BumpAllocLarge](BumpAllocLarge.md)
  - [BumpStats](BumpStats.md)
  - [BumpCheck](BumpCheck.md)
  - [BumpIsEmpty](BumpIsEmpty.md)

## Notes and Other Information
- The bump allocation strategy provides O(1) allocation time but does not support individual chunk deallocation
- Block sizes typically grow exponentially up to maxBlockSize to reduce the number of block allocations for large working sets
- The context is designed for temporary memory usage patterns where bulk reset is more common than individual frees
- Memory allocated from this context cannot be individually freed - only the entire context can be reset or deleted
- The allocChunkLimit determines when allocations should be handled as 'large' allocations versus regular bump allocations