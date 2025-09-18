# gistRelocateBuildBuffersOnSplit

## Location
[src/backend/access/gist/gistbuildbuffers.c:533-749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L533-L749)

## Overview
Redistributes buffered tuples from a split page to the appropriate new buffer pages during GiST index construction, using penalty-based page selection.

## Definition
```c
void gistRelocateBuildBuffersOnSplit(GISTBuildBuffers *gfbb, GISTSTATE *giststate,
                                   Relation r, int level, Buffer buffer, List *splitinfo)
```

## Detailed Description
This complex function handles one of the most critical aspects of GiST index construction: redistributing buffered tuples when a page splits. When a node splits during index construction, any tuples buffered for that node must be redistributed to the appropriate new nodes created by the split.

The function implements a sophisticated tuple placement algorithm similar to gistchoose(), computing penalties for each possible target page and selecting the one with the minimum penalty. It processes each tuple by examining all index key attributes and finding the page that results in the lowest penalty, with ties broken by examining subsequent attributes.

The function also handles the complex memory management involved in buffer relocation, creating temporary copies of the original buffer and properly initializing new buffers for the split pages.

## Parameters / Member Variables
- `gfbb`: Pointer to the GiST build buffers structure containing global build state
- `giststate`: GiST state information for the index being built
- `r`: The relation (index) being constructed
- `level`: The tree level at which the split is occurring
- `buffer`: The buffer containing the page that was split
- `splitinfo`: List of information about the pages created by the split

## Dependencies
- Functions called/Symbols referenced:
  - LEVEL_HAS_BUFFERS
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [hash_search](../h/hash_search.md)
  - gistDeCompressAtt
  - gistGetNodeBuffer
  - [gistPopItupFromNodeBuffer](gistPopItupFromNodeBuffer.md)
  - [gistpenalty](gistpenalty.md)
  - [gistPushItupToNodeBuffer](gistPushItupToNodeBuffer.md)
  - gistgetadjusted
  - IndexRelationGetNumberOfKeyAttributes
- Called from (representative examples):
  - gistbufferinginserttuples

## Notes and Other Information
- Returns early if the level doesn't use buffers or if no buffer exists for the split page
- Uses a penalty-based algorithm identical to the page selection logic in gistchoose()
- Handles the complex case where the leftmost split page reuses the original buffer
- Updates downlink tuples when necessary to maintain index consistency
- Critical for maintaining buffer organization during dynamic page splits
- Implements sophisticated memory management to handle buffer relocation safely