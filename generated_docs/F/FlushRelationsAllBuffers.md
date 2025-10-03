# FlushRelationsAllBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4580-4679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4580-L4679)

## Overview
Flushes all dirty pages from the buffer pool for multiple specified relations, equivalent to calling FlushRelationBuffers for each relation but optimized for bulk operations.

## Definition

```c
void
FlushRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
```
## Detailed Description
This function efficiently flushes all dirty pages belonging to multiple relations from the shared buffer pool to disk. It is optimized for bulk operations by using either linear search (for small numbers of relations) or binary search (for larger numbers) to identify matching buffers. The function assumes that all specified relations use shared buffers rather than local buffers (i.e., they are not temporary relations).

The function creates a sorted array of relation file locators when the number of relations exceeds RELS_BSEARCH_THRESHOLD, enabling efficient binary search during buffer scanning. For each buffer that matches one of the target relations and is both valid and dirty, it follows the standard buffer management protocol: pin the buffer, acquire content lock, flush to disk, release lock, and unpin.

The optimization strategy switches between linear search (O(n*m) where n=buffers, m=relations) and binary search (O(n*log(m))) based on the number of relations, similar to the approach used in DropRelationsAllBuffers.

## Parameters / Member Variables
- `*smgrs`: Array of SMgrRelation pointers representing the relations whose buffers should be flushed
- `nrels`: Number of relations in the smgrs array
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - RelFileLocatorBackendIsTemp
  - qsort
  - [rlocator_comparator](../r/rlocator_comparator.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - bsearch
  - [ReservePrivateRefCountEntry](../R/ReservePrivateRefCountEntry.md)
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [PinBuffer_Locked](../P/PinBuffer_Locked.md)
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [FlushBuffer](FlushBuffer.md)
  - [UnpinBuffer](../U/UnpinBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - [pfree](../p/pfree.md)
- Constants used:
  - RELS_BSEARCH_THRESHOLD
  - BM_VALID, BM_DIRTY
  - IOOBJECT_RELATION, IOCONTEXT_NORMAL
  - LW_SHARED
- Types used:
  - SMgrRelation, SMgrSortArray, BufferDesc, RelFileLocator
- Called from (representative examples):
  - [smgrdosyncall](../s/smgrdosyncall.md)

## Notes and Other Information
- Optimized for bulk operations - more efficient than calling FlushRelationBuffers repeatedly
- Uses adaptive search strategy: linear search for few relations (≤ RELS_BSEARCH_THRESHOLD), binary search for many
- Assumes all specified relations are permanent (not temporary) and use shared buffers
- Includes assertion to verify relations are not temporary (RelFileLocatorBackendIsTemp)
- Uses the same unlocked precheck optimization as other buffer management functions
- Sorts the relation array using qsort and rlocator_comparator when binary search is used
- Follows proper buffer management protocol with pinning, locking, and resource ownership
- Memory management: allocates temporary array with palloc and frees it with pfree
- The RELS_BSEARCH_THRESHOLD optimization point balances linear vs binary search costs
- Each buffer flush uses the standard FlushBuffer function with appropriate I/O context

## Simplified Source

```c
void FlushRelationsAllBuffers(SMgrRelation *smgrs, int nrels)
{
    int i;
    SMgrSortArray *srels;
    bool use_bsearch;

    if (nrels == 0)
        return;

    // Fill-in array for qsort
    srels = palloc(sizeof(SMgrSortArray) * nrels);

    for (i = 0; i < nrels; i++)
    {
        Assert(!RelFileLocatorBackendIsTemp(smgrs[i]->smgr_rlocator));
        srels[i].rlocator = smgrs[i]->smgr_rlocator.locator;
        srels[i].srel = smgrs[i];
    }

    // Use binary search for many relations
    use_bsearch = nrels > RELS_BSEARCH_THRESHOLD;
    if (use_bsearch)
        qsort(srels, nrels, sizeof(SMgrSortArray), rlocator_comparator);

    // Scan all buffers
    for (i = 0; i < NBuffers; i++)
    {
        SMgrSortArray *srelent = NULL;
        BufferDesc *bufHdr = GetBufferDescriptor(i);
        uint32 buf_state;

        // Find matching relation
        if (!use_bsearch)
        {
            for (int j = 0; j < nrels; j++)
            {
                if (BufTagMatchesRelFileLocator(&bufHdr->tag, &srels[j].rlocator))
                {
                    srelent = &srels[j];
                    break;
                }
            }
        }
        else
        {
            RelFileLocator rlocator = BufTagGetRelFileLocator(&bufHdr->tag);
            srelent = bsearch(&rlocator, srels, nrels, sizeof(SMgrSortArray),
                             rlocator_comparator);
        }

        if (srelent == NULL)
            continue;

        // Prepare for buffer operations
        ReservePrivateRefCountEntry();
        ResourceOwnerEnlarge(CurrentResourceOwner);

        buf_state = LockBufHdr(bufHdr);
        if (BufTagMatchesRelFileLocator(&bufHdr->tag, &srelent->rlocator) &&
            (buf_state & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
        {
            PinBuffer_Locked(bufHdr);
            LWLockAcquire(BufferDescriptorGetContentLock(bufHdr), LW_SHARED);
            FlushBuffer(bufHdr, srelent->srel, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
            LWLockRelease(BufferDescriptorGetContentLock(bufHdr));
            UnpinBuffer(bufHdr);
        }
        else
            UnlockBufHdr(bufHdr, buf_state);
    }

    pfree(srels);
}
```