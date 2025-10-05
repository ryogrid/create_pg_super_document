# brinGetStats

## Location
[src/backend/access/brin/brin.c:1639-1659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1639-L1659)

## Overview
Fetches statistical data from a BRIN index's metadata page and populates the provided BrinStatsData structure.

## Definition

```c
void
brinGetStats(Relation index, BrinStatsData *stats)
```
## Detailed Description
This function reads the metadata page of a BRIN index to extract essential statistical information. It accesses the first block of the index (the metadata page), reads the metadata structure, and extracts key statistics including the number of pages per range and the number of revmap pages. The function handles proper buffer locking to ensure safe concurrent access to the metadata page.

## Parameters / Member Variables
- `index`: The BRIN index relation from which to extract statistics
- `*stats`: Pointer to a BrinStatsData structure that will be populated with the index statistics
## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [LockBuffer](../L/LockBuffer.md) 
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetContents](../P/PageGetContents.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - BRIN_METAPAGE_BLKNO (constant)
  - BUFFER_LOCK_SHARE (constant)
- Types referenced:
  - [BrinStatsData](../B/BrinStatsData.md)
  - [BrinMetaPageData](../B/BrinMetaPageData.md)
- Called from (representative examples):
  - [brincostestimate](brincostestimate.md)
  - BrinGetAutoSummarize

## Notes and Other Information
- The function uses shared buffer locking to safely read the metadata page
- Statistics extracted include pagesPerRange and revmapNumPages which are critical for BRIN index cost estimation
- The revmap page count is calculated as (lastRevmapPage - 1) from the metadata
- This function is typically used by the query planner to estimate costs for BRIN index scans

## Simplified Source

```c
void
brinGetStats(Relation index, BrinStatsData *stats)
{
    Buffer metabuffer;
    Page metapage;
    BrinMetaPageData *metadata;

    // Read and lock the metadata page
    metabuffer = ReadBuffer(index, BRIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, BUFFER_LOCK_SHARE);
    metapage = BufferGetPage(metabuffer);
    metadata = (BrinMetaPageData *) PageGetContents(metapage);

    // Extract statistics from metadata
    stats->pagesPerRange = metadata->pagesPerRange;
    stats->revmapNumPages = metadata->lastRevmapPage - 1;

    // Release buffer
    UnlockReleaseBuffer(metabuffer);
}
```