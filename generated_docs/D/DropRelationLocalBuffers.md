# DropRelationLocalBuffers

## Location
[src/backend/storage/buffer/localbuf.c:489-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L489-L536)

## Overview
Removes specified pages of a relation from the local buffer pool, starting from a given block number, without writing dirty pages to disk first.

## Definition
void DropRelationLocalBuffers(RelFileLocator rlocator, ForkNumber forkNum, BlockNumber firstDelBlock)

## Detailed Description
This function removes all pages of a specified relation from the local buffer pool that have block numbers greater than or equal to firstDelBlock. It's used when truncating or dropping temporary tables and indexes. The function is particularly dangerous because it discards dirty pages without writing them to disk first, making it non-rollback-able.

The function iterates through all local buffers, identifies those belonging to the specified relation and fork, and removes them from both the buffer pool and the local buffer hash table. It performs safety checks to ensure no buffers are still referenced before removal, as this would indicate a serious bug in buffer management.

## Parameters / Member Variables
- `rlocator`: RelFileLocator identifying the relation whose buffers should be dropped
- `forkNum`: Fork number (main, FSM, VM, etc.) to target for buffer removal
- `firstDelBlock`: Starting block number - all blocks >= this value will be dropped (use 0 to drop all blocks)

## Dependencies
- Functions called/Symbols referenced:
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md)
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - relpathbackend
  - [hash_search](../h/hash_search.md)
  - [ClearBufferTag](../C/ClearBufferTag.md)
  - [pg_atomic_unlocked_write_u32](../p/pg_atomic_unlocked_write_u32.md)
- Called from (representative examples):
  - [DropRelationBuffers](DropRelationBuffers.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- WARNING: This function is NOT rollback-able - dirty pages are discarded without being written to disk
- Should only be used with extreme caution, typically during relation truncation or dropping
- Throws ERROR if any buffer is still referenced, indicating a buffer management bug
- Removes buffers from both the buffer pool and LocalBufHash hash table
- Only operates on local buffers (temporary tables/indexes visible to current session)
- Uses atomic operations to safely read and modify buffer state
- Validates buffer tag matches before removal to ensure correctness

## Simplified Source

```c
void DropRelationLocalBuffers(RelFileLocator rlocator, ForkNumber forkNum, BlockNumber firstDelBlock)
{
    int i;

    for (i = 0; i < NLocBuffer; i++) {
        BufferDesc *bufHdr = GetLocalBufferDescriptor(i);
        LocalBufferLookupEnt *hresult;
        uint32 buf_state;

        buf_state = pg_atomic_read_u32(&bufHdr->state);

        if ((buf_state & BM_TAG_VALID) &&
            BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator) &&
            BufTagGetForkNum(&bufHdr->tag) == forkNum &&
            bufHdr->tag.blockNum >= firstDelBlock) {

            if (LocalRefCount[i] != 0)
                elog(ERROR, "block %u of %s is still referenced (local %u)",
                     bufHdr->tag.blockNum,
                     relpathbackend(BufTagGetRelFileLocator(&bufHdr->tag),
                                   MyProcNumber,
                                   BufTagGetForkNum(&bufHdr->tag)),
                     LocalRefCount[i]);

            // Remove entry from hashtable
            hresult = (LocalBufferLookupEnt *)
                hash_search(LocalBufHash, &bufHdr->tag, HASH_REMOVE, NULL);
            if (!hresult)     /* shouldn't happen */
                elog(ERROR, "local buffer hash table corrupted");

            // Mark buffer invalid
            ClearBufferTag(&bufHdr->tag);
            buf_state &= ~BUF_FLAG_MASK;
            buf_state &= ~BUF_USAGECOUNT_MASK;
            pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
        }
    }
}
```