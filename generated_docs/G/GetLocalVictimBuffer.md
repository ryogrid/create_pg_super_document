# GetLocalVictimBuffer

## Location
[src/backend/storage/buffer/localbuf.c:177-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L177-L289)

## Overview
GetLocalVictimBuffer selects and prepares a local buffer for reuse using a clock sweep algorithm, handling dirty page write-out and buffer state transitions as needed.

## Definition

```c
static Buffer
GetLocalVictimBuffer(void)
```
## Detailed Description
GetLocalVictimBuffer implements buffer replacement policy for local buffers using a clock sweep algorithm similar to the main buffer manager. The function searches for an unpinned buffer with zero usage count, decrementing usage counts as it encounters buffers that are still 'warm' in the cache. When a suitable victim is found, it handles several critical tasks:

1. **Lazy allocation**: Allocates physical storage for the buffer if not already done
2. **Dirty page handling**: Writes dirty pages to disk before reusing the buffer, including checksum calculation and I/O statistics tracking
3. **Hash table maintenance**: Removes the old buffer tag from the local buffer hash table if it was valid
4. **State cleanup**: Clears buffer flags and resets the buffer to an invalid state

The function ensures resource ownership tracking and includes safety checks to prevent corruption of the local buffer hash table.

## Parameters
None (static function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md): Ensures resource owner can track additional buffer
  - [GetLocalBufferDescriptor](GetLocalBufferDescriptor.md): Converts buffer ID to BufferDesc pointer
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)/pg_atomic_unlocked_write_u32: Atomic state operations
  - [PinLocalBuffer](../P/PinLocalBuffer.md): Pins the selected victim buffer
  - LocalBufHdrGetBlock: Gets/sets the buffer's data page pointer
  - [GetLocalBufferStorage](GetLocalBufferStorage.md): Allocates physical storage for buffer
  - [smgropen](../s/smgropen.md): Opens storage manager relation for dirty page write-out
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md): Calculates and sets page checksum before writing
  - [smgrwrite](../s/smgrwrite.md): Performs actual disk write of dirty page
  - [hash_search](../h/hash_search.md): Removes old buffer tag from hash table
  - [ClearBufferTag](../C/ClearBufferTag.md)/BufferDescriptorGetBuffer: Buffer tag and descriptor utilities
  - Various I/O statistics functions (pgstat_prepare_io_time, pgstat_count_io_op_time, etc.)
- Called from (representative examples):
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md): Uses this to get victim when allocating new local buffer
  - [ExtendBufferedRelLocal](../E/ExtendBufferedRelLocal.md): Uses this when extending buffered relations locally

## Notes and Other Information
- Implements clock sweep replacement algorithm with usage count-based aging
- Uses lazy memory allocation - physical storage allocated only on first use
- Handles dirty page write-out with proper checksumming and I/O timing statistics
- Includes error handling for resource exhaustion (no available buffers)
- Tracks buffer usage statistics for temporary relation I/O operations
- Buffer state transitions are handled atomically to maintain consistency
- The trycounter mechanism prevents infinite loops when all buffers are pinned
- Part of PostgreSQL's local buffer management optimized for temporary relations performance

## Simplified Source

```c
static Buffer
GetLocalVictimBuffer(void)
{
    int victim_bufid;
    int trycounter;
    uint32 buf_state;
    BufferDesc *bufHdr;

    ResourceOwnerEnlarge(CurrentResourceOwner);

    // Clock sweep algorithm to find victim buffer
    trycounter = NLocBuffer;
    for (;;)
    {
        victim_bufid = nextFreeLocalBufId;

        // Wrap around buffer ID counter
        if (++nextFreeLocalBufId >= NLocBuffer)
            nextFreeLocalBufId = 0;

        bufHdr = GetLocalBufferDescriptor(victim_bufid);

        // Check if buffer is unpinned (reference count = 0)
        if (LocalRefCount[victim_bufid] == 0)
        {
            buf_state = pg_atomic_read_u32(&bufHdr->state);

            // Decrement usage count if buffer is still "hot"
            if (BUF_STATE_GET_USAGECOUNT(buf_state) > 0)
            {
                buf_state -= BUF_USAGECOUNT_ONE;
                pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
                trycounter = NLocBuffer;  // Reset try counter
            }
            else
            {
                // Found usable victim buffer
                PinLocalBuffer(bufHdr, false);
                break;
            }
        }
        else if (--trycounter == 0)
        {
            // All buffers are pinned - error out
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                           errmsg("no empty local buffer available")));
        }
    }

    // Lazy memory allocation - allocate storage on first use
    if (LocalBufHdrGetBlock(bufHdr) == NULL)
    {
        LocalBufHdrGetBlock(bufHdr) = GetLocalBufferStorage();
    }

    // Write out dirty buffer before reusing
    if (buf_state & BM_DIRTY)
    {
        SMgrRelation oreln;
        Page localpage = (char *) LocalBufHdrGetBlock(bufHdr);

        // Open relation and write dirty page to disk
        oreln = smgropen(BufTagGetRelFileLocator(&bufHdr->tag), MyProcNumber);
        PageSetChecksumInplace(localpage, bufHdr->tag.blockNum);

        smgrwrite(oreln,
                  BufTagGetForkNum(&bufHdr->tag),
                  bufHdr->tag.blockNum,
                  localpage,
                  false);

        // Update I/O statistics and clear dirty flag
        buf_state &= ~BM_DIRTY;
        pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
        pgBufferUsage.local_blks_written++;
    }

    // Remove old buffer tag from hash table if valid
    if (buf_state & BM_TAG_VALID)
    {
        LocalBufferLookupEnt *hresult;

        hresult = (LocalBufferLookupEnt *)
            hash_search(LocalBufHash, &bufHdr->tag, HASH_REMOVE, NULL);

        if (!hresult)
            elog(ERROR, "local buffer hash table corrupted");

        // Clear buffer tag and reset state
        ClearBufferTag(&bufHdr->tag);
        buf_state &= ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        pg_atomic_unlocked_write_u32(&bufHdr->state, buf_state);
    }

    return BufferDescriptorGetBuffer(bufHdr);
}
```