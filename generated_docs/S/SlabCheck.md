# SlabCheck

## Location
[src/backend/utils/mmgr/slab.c:997-1154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/slab.c#L997-L1154)

## Overview
SlabCheck performs comprehensive integrity validation of a Slab memory context by walking through all blocks and chunks to detect memory corruption and inconsistencies.

## Definition

```c
void
SlabCheck(MemoryContext context)
```
## Detailed Description
SlabCheck is a diagnostic function that thoroughly validates the internal consistency of a Slab memory context. It performs multiple levels of validation including block list integrity, chunk accounting accuracy, free list consistency, and memory boundary checks. The function is designed to detect various forms of memory corruption including incorrect block placement, invalid free list links, chunk header corruption, and buffer overruns.

The validation process includes checking empty blocks for correct free chunk counts, verifying that blocks are placed on appropriate blocklists based on their free chunk count, validating free list pointers and chunk alignment, checking unused chunk tracking, and verifying chunk headers and sentinel bytes for allocated chunks. All errors are reported as WARNING level messages rather than ERROR or FATAL to prevent infinite recursion during error handling.

## Parameters / Member Variables
- `context`: The MemoryContext to validate (cast internally to SlabContext)
## Dependencies
- Functions called/Symbols referenced:
  - SlabIsValid
  - dclist_foreach
  - dlist_container
  - [SlabBlocklistIndex](SlabBlocklistIndex.md)
  - SlabChunkIndex
  - SlabBlockGetChunk
  - SlabChunkMod
  - MemoryChunkGetPointer
  - SlabChunkGetPointer
  - [MemoryChunkGetBlock](../M/MemoryChunkGetBlock.md)
  - [sentinel_ok](../s/sentinel_ok.md)
  - [dclist_count](../d/dclist_count.md)
  - VALGRIND_MAKE_MEM_DEFINED
  - VALGRIND_MAKE_MEM_NOACCESS
- Called from (representative examples):
  - [SlabReset](SlabReset.md)
  - Memory context debugging routines

## Notes and Other Information
This function uses WARNING level logging rather than ERROR/FATAL to prevent infinite loops during error cleanup when memory corruption is detected. It maintains a temporary isChunkFree array to track chunk states during validation. The function performs Valgrind-aware memory access control, temporarily allowing access to chunk headers for validation then disabling access again. Sentinel byte checking helps detect buffer overruns in allocated chunks. This is a critical debugging tool for diagnosing Slab allocator issues and memory corruption problems in PostgreSQL.

## Simplified Source

```c
void SlabCheck(MemoryContext context)
{
    SlabContext *slab = (SlabContext *) context;
    int nblocks = 0;
    const char *name = slab->header.name;

    // Validate empty blocks - should have all chunks free
    dclist_iter iter;
    dclist_foreach(iter, &slab->emptyblocks) {
        SlabBlock *block = dlist_container(SlabBlock, node, iter.cur);
        if (block->nfree != slab->chunksPerBlock) {
            elog(WARNING, "problem in slab %s: empty block %p should have %d free chunks but has %d",
                 name, block, slab->chunksPerBlock, block->nfree);
        }
    }

    // Walk through all non-empty block lists
    for (int i = 0; i < SLAB_BLOCKLIST_COUNT; i++) {
        dlist_foreach(iter, &slab->blocklist[i]) {
            SlabBlock *block = dlist_container(SlabBlock, node, iter.cur);
            int nfree = 0;

            // Verify block is on correct blocklist
            if (SlabBlocklistIndex(slab, block->nfree) != i) {
                elog(WARNING, "problem in slab %s: block %p on wrong blocklist", name, block);
            }

            // Verify block is not empty and has correct slab pointer
            if (block->nfree >= slab->chunksPerBlock || block->slab != slab) {
                elog(WARNING, "problem in slab %s: invalid block %p", name, block);
            }

            // Reset chunk tracking array
            memset(slab->isChunkFree, 0, slab->chunksPerBlock * sizeof(bool));

            // Walk free list and validate chunks
            MemoryChunk *cur_chunk = block->freehead;
            while (cur_chunk != NULL) {
                int chunkidx = SlabChunkIndex(slab, block, cur_chunk);

                // Validate chunk address and alignment
                if (cur_chunk < SlabBlockGetChunk(slab, block, 0) ||
                    cur_chunk > SlabBlockGetChunk(slab, block, slab->chunksPerBlock - 1) ||
                    SlabChunkMod(slab, block, cur_chunk) != 0) {
                    elog(WARNING, "problem in slab %s: bogus free list link %p", name, cur_chunk);
                }

                nfree++;
                slab->isChunkFree[chunkidx] = true;
                cur_chunk = *(MemoryChunk **) SlabChunkGetPointer(cur_chunk);
            }

            // Validate unused chunk tracking
            if (SlabBlockGetChunk(slab, block, slab->chunksPerBlock - block->nunused) != block->unused) {
                elog(WARNING, "problem in slab %s: unused pointer mismatch in block %p", name, block);
            }

            // Count unused chunks
            cur_chunk = block->unused;
            for (int j = 0; j < block->nunused; j++) {
                int chunkidx = SlabChunkIndex(slab, block, cur_chunk);
                nfree++;
                if (chunkidx < slab->chunksPerBlock) {
                    slab->isChunkFree[chunkidx] = true;
                }
                cur_chunk = (MemoryChunk *) (((char *) cur_chunk) + slab->fullChunkSize);
            }

            // Validate allocated chunks
            for (int j = 0; j < slab->chunksPerBlock; j++) {
                if (!slab->isChunkFree[j]) {
                    MemoryChunk *chunk = SlabBlockGetChunk(slab, block, j);
                    SlabBlock *chunkblock = (SlabBlock *) MemoryChunkGetBlock(chunk);

                    // Verify chunk points back to correct block
                    if (chunkblock != block) {
                        elog(WARNING, "problem in slab %s: bogus block link in chunk %p", name, chunk);
                    }

                    // Check for buffer overruns using sentinel
                    if (!sentinel_ok(chunk, Slab_CHUNKHDRSZ + slab->chunkSize)) {
                        elog(WARNING, "problem in slab %s: write past chunk end detected", name);
                    }
                }
            }

            // Verify free chunk count is accurate
            if (nfree != block->nfree) {
                elog(WARNING, "problem in slab %s: nfree mismatch in block %p", name, block);
            }

            nblocks++;
        }
    }

    // Verify total allocated memory accounting
    nblocks += dclist_count(&slab->emptyblocks);
    Assert(nblocks * slab->blockSize == context->mem_allocated);
}
```