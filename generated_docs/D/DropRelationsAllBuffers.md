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