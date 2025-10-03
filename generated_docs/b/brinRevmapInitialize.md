# brinRevmapInitialize

## Location
[src/backend/access/brin/brin_revmap.c:70-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L70-L99)

## Overview
Initializes an access object for a BRIN (Block Range Index) range map, which provides the mapping from heap block ranges to index tuples.

## Definition

```c
BrinRevmap *
brinRevmapInitialize(Relation idxrel, BlockNumber *pagesPerRange)
```
## Detailed Description
This function creates and initializes a BrinRevmap structure that serves as the access interface to a BRIN index's range map. The range map is a critical component of BRIN indexes that maintains the mapping between heap block ranges and their corresponding index tuples. The function reads the index's metadata page to extract essential parameters like pages per range and the last revmap page number, then constructs the access object with this information.

The function performs the following key operations:
1. Reads and locks the BRIN metadata page to access index configuration
2. Extracts metadata including pages per range and last revmap page
3. Allocates and initializes a BrinRevmap structure
4. Sets up the revmap with index relation, range parameters, and buffer management
5. Returns the configured revmap object for subsequent operations

## Parameters / Member Variables
- : The BRIN index relation for which to initialize the revmap
- : Output parameter that receives the number of heap pages covered by each index range

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md)  
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetContents](../P/PageGetContents.md)
  - [palloc](../p/palloc.md)
- Types referenced:
  - [BrinRevmap](../B/BrinRevmap.md)
  - [BrinMetaPageData](../B/BrinMetaPageData.md)
  - BRIN_METAPAGE_BLKNO
  - BUFFER_LOCK_SHARE
  - BUFFER_LOCK_UNLOCK
- Called from:
  - [initialize_brin_insertstate](../i/initialize_brin_insertstate.md)
  - [brinbeginscan](brinbeginscan.md)
  - [brinbuild](brinbuild.md)
  - [brinsummarize](brinsummarize.md)
  - [brinRevmapDesummarizeRange](brinRevmapDesummarizeRange.md)

## Notes and Other Information
- The returned BrinRevmap object must be freed using brinRevmapTerminate when no longer needed
- The function holds a shared lock on the metadata page during initialization but releases it before returning
- The metadata buffer (rm_metaBuf) remains held in the revmap structure for later use
- This is typically the first function called when beginning any BRIN revmap operations

## Simplified Source

```c
BrinRevmap *brinRevmapInitialize(Relation idxrel, BlockNumber *pagesPerRange)
{
    BrinRevmap *revmap;
    Buffer meta;
    BrinMetaPageData *metadata;
    Page page;

    // Read and lock metadata page
    meta = ReadBuffer(idxrel, BRIN_METAPAGE_BLKNO);
    LockBuffer(meta, BUFFER_LOCK_SHARE);
    page = BufferGetPage(meta);
    metadata = (BrinMetaPageData *) PageGetContents(page);

    // Initialize revmap structure
    revmap = palloc(sizeof(BrinRevmap));
    revmap->rm_irel = idxrel;
    revmap->rm_pagesPerRange = metadata->pagesPerRange;
    revmap->rm_lastRevmapPage = metadata->lastRevmapPage;
    revmap->rm_metaBuf = meta;
    revmap->rm_currBuf = InvalidBuffer;

    // Return pages per range to caller
    *pagesPerRange = metadata->pagesPerRange;

    LockBuffer(meta, BUFFER_LOCK_UNLOCK);

    return revmap;
}
```