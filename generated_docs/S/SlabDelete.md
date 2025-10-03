# SlabDelete

## Location
[src/backend/utils/mmgr/slab.c:485-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L485-L497)

## Overview
SlabDelete is a memory context deletion function that completely destroys a slab memory context by freeing all allocated memory and the context header itself.

## Definition

```c
void
SlabDelete(MemoryContext context)
```
## Detailed Description
SlabDelete is responsible for completely destroying a slab memory context. It performs a two-step deletion process: first it calls SlabReset to free all the memory blocks allocated within the context, then it frees the context header structure itself using the standard free() function. This is the cleanup function used when a slab memory context is no longer needed and should be completely removed from memory.

## Parameters / Member Variables
- `context`: The MemoryContext (slab context) to be deleted and freed
## Dependencies
- Functions called/Symbols referenced:
  - [SlabReset](SlabReset.md)
  - free
- Called from (representative examples):
  - BOGUS_MCTX (src/backend/utils/mmgr/mcxt.c:80)
  - Referenced in MEMUTILS_INTERNAL_H (src/include/utils/memutils_internal.h:61)

## Notes and Other Information
- This function follows the standard PostgreSQL memory context deletion pattern: reset first, then free the context structure
- The function is part of the slab memory allocator implementation in PostgreSQL
- After calling this function, the context pointer becomes invalid and should not be used
- Located in src/backend/utils/mmgr/slab.c:485-497