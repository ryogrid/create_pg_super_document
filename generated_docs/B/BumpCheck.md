# BumpCheck

## Location
[src/backend/utils/mmgr/bump.c:738-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/bump.c#L738-L808)

## Overview
BumpCheck performs comprehensive consistency checking of a Bump memory context by walking through all blocks and chunks to verify memory structure integrity and detect corruption.

## Definition

```c
void
BumpCheck(MemoryContext context)
```
## Detailed Description
This function implements thorough memory consistency checking for the Bump allocator by iterating through all blocks in the context and examining each allocated chunk within those blocks. It validates that block-context relationships are correct, chunk headers are valid, and memory layout is consistent. The function performs several critical checks including verifying that external chunks don't coexist with other chunks in the same block, that chunk block pointers are correct, and that total allocated memory matches the context's accounting. Any inconsistencies are reported as WARNING messages rather than errors to avoid infinite loops during error recovery.

## Parameters / Member Variables
- `context`: The MemoryContext to check for consistency (cast internally to BumpContext)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (block iteration)
  - dlist_container (container extraction)
  - IsKeeperBlock (block type checking)
  - elog (warning reporting)
  - VALGRIND_MAKE_MEM_DEFINED (valgrind support)
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md) (chunk type checking)
  - ExternalChunkGetBlock (external chunk handling)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md) (chunk block extraction)
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md) (chunk size retrieval)
  - MemoryChunkGetPointer (chunk pointer extraction)
- Called from (representative examples):
  - [BumpReset](BumpReset.md) (during reset operations)
  - BOGUS_MCTX (via function pointer table)
  - Memory context debugging functions

## Notes and Other Information
- Reports problems as WARNING level to prevent infinite loops during error handling
- Validates that external chunks are alone in dedicated blocks
- Includes Valgrind memory debugging support via VALGRIND_MAKE_MEM_DEFINED
- Tracks total allocated memory and asserts it matches context accounting
- Walks chunk headers using Bump_CHUNKHDRSZ for proper alignment
- Essential for debugging memory corruption issues in the Bump allocator
- Located in src/backend/utils/mmgr/bump.c:738-808

## Simplified Source

```c
void
BumpCheck(MemoryContext context)
{
    BumpContext *bump = (BumpContext *) context;
    const char *name = context->name;
    dlist_iter iter;
    Size total_allocated = 0;

    // Walk all blocks in this context
    dlist_foreach(iter, &bump->blocks)
    {
        BumpBlock *block = dlist_container(BumpBlock, node, iter.cur);
        int nchunks = 0;
        char *ptr;
        bool has_external_chunk = false;

        // Calculate block's allocated size
        if (IsKeeperBlock(bump, block))
            total_allocated += block->endptr - (char *) bump;
        else
            total_allocated += block->endptr - (char *) block;

        // Check block belongs to correct context
        if (block->context != bump)
            elog(WARNING, "problem in Bump %s: bogus context link in block %p", name, block);

        // Walk through chunks in this block
        ptr = ((char *) block) + Bump_BLOCKHDRSZ;
        while (ptr < block->freeptr)
        {
            MemoryChunk *chunk = (MemoryChunk *) ptr;
            BumpBlock *chunkblock;
            Size chunksize;

            VALGRIND_MAKE_MEM_DEFINED(chunk, Bump_CHUNKHDRSZ);

            // Handle external vs normal chunks
            if (MemoryChunkIsExternal(chunk))
            {
                chunkblock = ExternalChunkGetBlock(chunk);
                chunksize = block->endptr - (char *) MemoryChunkGetPointer(chunk);
                has_external_chunk = true;
            }
            else
            {
                chunkblock = MemoryChunkGetBlock(chunk);
                chunksize = MemoryChunkGetValue(chunk);
            }

            // Move to next chunk and validate
            ptr += (chunksize + Bump_CHUNKHDRSZ);
            nchunks++;

            if (chunkblock != block)
                elog(WARNING, "problem in Bump %s: bogus block link in block %p, chunk %p",
                     name, block, chunk);
        }

        // External chunks should be alone in their block
        if (has_external_chunk && nchunks > 1)
            elog(WARNING, "problem in Bump %s: external chunk on non-dedicated block %p",
                 name, block);
    }

    Assert(total_allocated == context->mem_allocated);
}
```