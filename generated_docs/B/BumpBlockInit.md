# BumpBlockInit

## Location
[src/backend/utils/mmgr/bump.c:535-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L535-L551)

## Overview
BumpBlockInit initializes a newly allocated bump memory block by setting up its internal pointers and marking unallocated memory for debugging tools.

## Definition

```c
static inline void
BumpBlockInit(BumpContext *context, BumpBlock *block, Size blksize)
```
## Detailed Description
This function performs the essential initialization of a bump memory block after it has been allocated but before it can be used for chunk allocations. It sets up the free pointer to point just after the block header, establishes the end pointer to mark the block boundary, and optionally stores a back-reference to the context for debugging. The function also integrates with Valgrind memory debugging by marking the unallocated portion of the block as NOACCESS to catch invalid memory access errors during development.

## Parameters / Member Variables
- : The bump context that owns this block (used for debugging builds)
- : The allocated memory block to initialize
- : The total size of the allocated block in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [BumpContext](BumpContext.md) (context structure type)
  - [BumpBlock](BumpBlock.md) (block structure type)
  - MEMORY_CONTEXT_CHECKING (conditional compilation for debugging features)
  - Bump_BLOCKHDRSZ (size of block header constant)
  - VALGRIND_MAKE_MEM_NOACCESS (Valgrind integration for memory debugging)
- Called from (representative examples):
  - ExternalChunkGetBlock (for external chunk block initialization)
  - [BumpContextCreate](BumpContextCreate.md) (for initial block setup)
  - [BumpAllocFromNewBlock](BumpAllocFromNewBlock.md) (for new block initialization)

## Notes and Other Information
- Marked as static inline for performance optimization
- Does not update the context's mem_allocated field (caller's responsibility)
- Conditionally stores context back-reference only in debug builds to save memory
- Integrates with Valgrind memory checking tools for development debugging
- Sets up free pointer alignment to start allocation immediately after block header
- Essential for proper block lifecycle management in bump allocator