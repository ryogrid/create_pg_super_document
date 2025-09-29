# DropRelationsAllBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4144-4314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4144-L4314)

## Overview
DropRelationsAllBuffers removes all pages of all forks from multiple relations from the buffer pool, equivalent to calling DropRelationBuffers with firstDelBlock = 0 for each fork of each relation.

## Definition
```c
void DropRelationsAllBuffers(SMgrRelation *smgr_reln, int nlocators)
```

## Detailed Description
DropRelationsAllBuffers efficiently removes all cached pages for multiple relations simultaneously. The function implements several optimizations: it separates local (temporary) from shared relations, uses cached relation sizes when available for targeted buffer lookup, and employs either linear search or binary search depending on the number of relations. For small total buffer counts (below BUF_DROP_FULL_SCAN_THRESHOLD), it uses FindAndDropRelationBuffers for each fork; otherwise, it performs a full buffer pool scan with optimized lookup strategies based on relation count.

## Parameters / Member Variables
- `smgr_reln`: Array of storage manager relations to process
- `nlocators`: Number of relations in the smgr_reln array

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocatorBackendIsTemp
  - [DropRelationAllLocalBuffers](DropRelationAllLocalBuffers.md)
  - [smgrnblocks_cached](../s/smgrnblocks_cached.md)
  - [smgrexists](../s/smgrexists.md)
  - BlockNumberIsValid
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - qsort
  - [rlocator_comparator](../r/rlocator_comparator.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [BufTagGetRelFileLocator](../B/BufTagGetRelFileLocator.md)
  - bsearch
  - [LockBufHdr](../L/LockBufHdr.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BUF_DROP_FULL_SCAN_THRESHOLD (constant)
  - RELS_BSEARCH_THRESHOLD (constant)
- Called from (representative examples):
  - [smgrdounlinkall](../s/smgrdounlinkall.md)

## Notes and Other Information
- Processes all relation forks (main, FSM, VM, etc.) for each relation
- Uses RELS_BSEARCH_THRESHOLD to decide between linear and binary search strategies
- Handles both shared and local (temporary) relations appropriately
- Memory-efficient: allocates arrays only for non-local relations
- Critical for bulk relation deletion operations where multiple relations need cleanup
- Like DropRelationBuffers, this is non-rollback-able and drops dirty pages without writing
- Implements sophisticated optimization logic to minimize buffer pool scanning overhead

## Simplified Source

```c
void DropRelationsAllBuffers(SMgrRelation *smgr_reln, int nlocators)
{
    int i;
    int n = 0;
    SMgrRelation *rels;
    BlockNumber (*block)[MAX_FORKNUM + 1];
    uint64 nBlocksToInvalidate = 0;
    RelFileLocator *locators;
    bool cached = true;
    bool use_bsearch;

    if (nlocators == 0)
        return;

    rels = palloc(sizeof(SMgrRelation) * nlocators);

    // Separate local (temp) relations from non-local ones
    for (i = 0; i < nlocators; i++)
    {
        if (RelFileLocatorBackendIsTemp(smgr_reln[i]->smgr_rlocator))
        {
            if (smgr_reln[i]->smgr_rlocator.backend == MyProcNumber)
                DropRelationAllLocalBuffers(smgr_reln[i]->smgr_rlocator.locator);
        }
        else
            rels[n++] = smgr_reln[i];
    }

    if (n == 0)
    {
        pfree(rels);
        return;
    }

    // Try optimization: collect exact block counts
    block = (BlockNumber (*)[MAX_FORKNUM + 1])
        palloc(sizeof(BlockNumber) * n * (MAX_FORKNUM + 1));

    for (i = 0; i < n && cached; i++)
    {
        for (int j = 0; j <= MAX_FORKNUM; j++)
        {
            block[i][j] = smgrnblocks_cached(rels[i], j);
            if (block[i][j] == InvalidBlockNumber)
            {
                if (!smgrexists(rels[i], j))
                    continue;
                cached = false;
                break;
            }
            nBlocksToInvalidate += block[i][j];
        }
    }

    // Use optimization if block count is reasonable
    if (cached && nBlocksToInvalidate < BUF_DROP_FULL_SCAN_THRESHOLD)
    {
        for (i = 0; i < n; i++)
        {
            for (int j = 0; j <= MAX_FORKNUM; j++)
            {
                if (!BlockNumberIsValid(block[i][j]))
                    continue;
                FindAndDropRelationBuffers(rels[i]->smgr_rlocator.locator,
                                          j, block[i][j], 0);
            }
        }
        pfree(block);
        pfree(rels);
        return;
    }

    // Fall back to full buffer pool scan
    pfree(block);
    locators = palloc(sizeof(RelFileLocator) * n);
    for (i = 0; i < n; i++)
        locators[i] = rels[i]->smgr_rlocator.locator;

    use_bsearch = n > RELS_BSEARCH_THRESHOLD;
    if (use_bsearch)
        qsort(locators, n, sizeof(RelFileLocator), rlocator_comparator);

    // Scan all buffers
    for (i = 0; i < NBuffers; i++)
    {
        BufferDesc *bufHdr = GetBufferDescriptor(i);
        RelFileLocator *rlocator = NULL;
        uint32 buf_state;

        // Find matching locator
        if (!use_bsearch)
        {
            for (int j = 0; j < n; j++)
            {
                if (BufTagMatchesRelFileLocator(&bufHdr->tag, &locators[j]))
                {
                    rlocator = &locators[j];
                    break;
                }
            }
        }
        else
        {
            RelFileLocator locator = BufTagGetRelFileLocator(&bufHdr->tag);
            rlocator = bsearch(&locator, locators, n, sizeof(RelFileLocator),
                              rlocator_comparator);
        }

        if (rlocator == NULL)
            continue;

        // Invalidate matching buffer
        buf_state = LockBufHdr(bufHdr);
        if (BufTagMatchesRelFileLocator(&bufHdr->tag, rlocator))
            InvalidateBuffer(bufHdr);
        else
            UnlockBufHdr(bufHdr, buf_state);
    }

    pfree(locators);
    pfree(rels);
}
```