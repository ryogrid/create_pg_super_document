# AllocSetCheck

## Location
[src/backend/utils/mmgr/aset.c:1599-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/aset.c#L1599-L1724)

## Overview
Performs comprehensive consistency checking of an AllocSet memory context by walking through all chunks and validating memory integrity.

## Definition
```c
void AllocSetCheck(MemoryContext context)
```

## Detailed Description
AllocSetCheck is a debugging and validation function that thoroughly examines an AllocSet memory context for consistency errors. It walks through all memory blocks and chunks, validating block headers, chunk sizes, alignment, and detecting memory corruption such as buffer overruns. The function checks that external chunks consume entire blocks, validates free list indices, ensures requested sizes don't exceed allocated sizes, and verifies block linkage. It uses Valgrind annotations during chunk inspection and reports all errors as WARNING level messages to avoid infinite loops during error recovery. The function is primarily used during context reset and deletion operations when MEMORY_CONTEXT_CHECKING is enabled.

## Parameters / Member Variables
- `context`: The MemoryContext to check for consistency and integrity

## Dependencies
- Functions called/Symbols referenced:
  - IsKeeperBlock
  - [MemoryChunkIsExternal](../M/MemoryChunkIsExternal.md)
  - MemoryChunkGetPointer
  - [MemoryChunkGetValue](../M/MemoryChunkGetValue.md)
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - FreeListIdxIsValid
  - GetChunkSizeFromFreeListIdx
  - [sentinel_ok](../s/sentinel_ok.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
  - elog
- Called from (representative examples):
  - [AllocSetReset](AllocSetReset.md)
  - [AllocSetDelete](AllocSetDelete.md)
  - BOGUS_MCTX (via function pointer assignment)
  - Referenced in memutils_internal.h

## Notes and Other Information
- Reports errors as WARNING level to prevent infinite loops during error cleanup
- Validates block header fields including prev/next linkage and free pointer boundaries
- Checks external chunks consume entire blocks and regular chunks have valid free list indices
- Detects buffer overruns by checking sentinel values in padding space
- Ensures total allocated memory matches context's mem_allocated counter
- Only active when MEMORY_CONTEXT_CHECKING is enabled
- Part of PostgreSQL's memory debugging and validation infrastructure
- Critical for detecting memory corruption in development and testing environments

## Simplified Source

```c
void
AllocSetCheck(MemoryContext context)
{
    AllocSet set = (AllocSet) context;
    const char *name = set->header.name;
    AllocBlock prevblock;
    AllocBlock block;
    Size total_allocated = 0;

    // Walk through all blocks in the context
    for (prevblock = NULL, block = set->blocks;
         block != NULL;
         prevblock = block, block = block->next)
    {
        char *bpoz = ((char *) block) + ALLOC_BLOCKHDRSZ;
        long blk_used = block->freeptr - bpoz;
        long blk_data = 0;
        long nchunks = 0;
        bool has_external_chunk = false;

        // Track total allocated memory
        if (IsKeeperBlock(set, block))
            total_allocated += block->endptr - ((char *) set);
        else
            total_allocated += block->endptr - ((char *) block);

        // Validate empty blocks
        if (!blk_used && !IsKeeperBlock(set, block))
            elog(WARNING, "problem in alloc set %s: empty block %p", name, block);

        // Check block header fields
        if (block->aset != set ||
            block->prev != prevblock ||
            block->freeptr < bpoz ||
            block->freeptr > block->endptr)
            elog(WARNING, "problem in alloc set %s: corrupt header in block %p", name, block);

        // Walk through all chunks in the block
        while (bpoz < block->freeptr)
        {
            MemoryChunk *chunk = (MemoryChunk *) bpoz;
            Size chsize, dsize;

            if (MemoryChunkIsExternal(chunk))
            {
                // External chunk should consume entire block
                chsize = block->endptr - (char *) MemoryChunkGetPointer(chunk);
                has_external_chunk = true;

                if (chsize + ALLOC_CHUNKHDRSZ != blk_used)
                    elog(WARNING, "problem in alloc set %s: bad single-chunk %p in block %p",
                         name, chunk, block);
            }
            else
            {
                // Regular chunk - validate free list index
                int fidx = MemoryChunkGetValue(chunk);

                if (!FreeListIdxIsValid(fidx))
                    elog(WARNING, "problem in alloc set %s: bad chunk size for chunk %p in block %p",
                         name, chunk, block);

                chsize = GetChunkSizeFromFreeListIdx(fidx);

                // Check block offset points to correct block
                if (block != MemoryChunkGetBlock(chunk))
                    elog(WARNING, "problem in alloc set %s: bad block offset for chunk %p in block %p",
                         name, chunk, block);
            }

            dsize = chunk->requested_size;

            // Validate requested size doesn't exceed chunk size
            if (dsize != InvalidAllocSize && dsize > chsize)
                elog(WARNING, "problem in alloc set %s: req size > alloc size for chunk %p in block %p",
                     name, chunk, block);

            // Check minimum chunk size
            if (chsize < (1 << ALLOC_MINBITS))
                elog(WARNING, "problem in alloc set %s: bad size %zu for chunk %p in block %p",
                     name, chsize, chunk, block);

            // Check for buffer overruns using sentinel
            if (dsize != InvalidAllocSize && dsize < chsize &&
                !sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dsize))
                elog(WARNING, "problem in alloc set %s: detected write past chunk end in block %p, chunk %p",
                     name, block, chunk);

            blk_data += chsize;
            nchunks++;

            bpoz += ALLOC_CHUNKHDRSZ + chsize;
        }

        // Validate block consistency
        if ((blk_data + (nchunks * ALLOC_CHUNKHDRSZ)) != blk_used)
            elog(WARNING, "problem in alloc set %s: found inconsistent memory block %p", name, block);

        if (has_external_chunk && nchunks > 1)
            elog(WARNING, "problem in alloc set %s: external chunk on non-dedicated block %p", name, block);
    }

    // Final validation: total memory should match context accounting
    Assert(total_allocated == context->mem_allocated);
}
```