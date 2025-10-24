# GenerationDelete

## Location
[src/backend/utils/mmgr/generation.c:328-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/generation.c#L328-L342)

## Overview
Completely destroys a Generation memory context by freeing all allocated memory and the context structure itself.

## Definition

```c
void
GenerationDelete(MemoryContext context)
```
## Detailed Description
GenerationDelete provides complete cleanup of a Generation memory context. It operates in two phases: first calling GenerationReset to free all releasable GenerationBlocks, then freeing the context header and keeper block using the standard free() function. This function ensures complete deallocation of all resources associated with the Generation context.

The implementation leverages GenerationReset to handle the complex logic of freeing individual blocks while preserving the keeper block, then simply frees the remaining memory that contains both the context header and the keeper block in one operation.

## Parameters / Member Variables
- `context`: The Generation memory context to delete and free completely
## Dependencies
- Functions called/Symbols referenced:
  - [GenerationReset](GenerationReset.md)
  - free
  - pg_noinline
- Called from (representative examples):
  - BOGUS_MCTX
  - MEMUTILS_INTERNAL_H

## Notes and Other Information
- The function performs a two-stage deletion: reset then free
- [GenerationReset](GenerationReset.md) handles freeing all non-keeper blocks
- The final free() call deallocates both the context header and keeper block together
- This function should only be called when the context is no longer needed
- The pg_noinline attribute suggests this function should not be inlined for performance or debugging reasons
- After calling this function, the context pointer becomes invalid and should not be used

## Simplified Source

```c
void GenerationDelete(MemoryContext context)
{
    // Reset context to free all releasable blocks
    GenerationReset(context);

    // Free the context header and keeper block
    free(context);
}
```