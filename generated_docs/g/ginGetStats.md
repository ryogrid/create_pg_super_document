# ginGetStats

## Location
src/backend/access/gin/ginutil.c: 623 - 649

## Overview
Retrieves statistical data from a GIN index's metadata page, providing current information about the index structure and contents for query planning and monitoring purposes.

## Definition
```c
void ginGetStats(Relation index, GinStatsData *stats)
```

## Detailed Description
The `ginGetStats` function reads statistical information from a GIN index's metadata page and populates a `GinStatsData` structure with current index statistics. It accesses the index's metapage (block 0) using a shared lock, extracts metadata from the `GinMetaPageData` structure, and copies key statistics including page counts, entry counts, and version information. The function provides both real-time data (like `nPendingPages` and `ginVersion`) and historical data (other fields reflect the state as of the last VACUUM operation). This information is crucial for query cost estimation and index monitoring.

## Parameters / Member Variables
- `index`: Relation pointer to the GIN index from which to retrieve statistics
- `stats`: Output parameter - pointer to a `GinStatsData` structure to be populated with index statistics

## Dependencies
- Functions called/Symbols referenced:
  - `GinStatsData` (structure for returning statistical data)
  - [GinMetaPageData](../G/GinMetaPageData.md) (structure containing metadata on the metapage)
  - [ReadBuffer](../R/ReadBuffer.md) (function to read a buffer from storage)
  - [LockBuffer](../L/LockBuffer.md)/`UnlockReleaseBuffer` (buffer locking functions)
  - [BufferGetPage](../B/BufferGetPage.md) (function to get page from buffer)
  - `GinPageGetMeta` (macro to extract metadata from a GIN metapage)
  - `GIN_METAPAGE_BLKNO` (constant for metapage block number)
  - `GIN_SHARE` (constant for shared lock mode)
- Called from (representative examples):
  - [ginNewScanKey](ginNewScanKey.md) (scan initialization)
  - [gincostestimate](gincostestimate.md) (query cost estimation)

## Notes and Other Information
- Acquires only a shared lock on the metapage, allowing concurrent reads
- The `nPendingPages` field can be trusted to be current and up-to-date
- Most other statistics reflect the state as of the last VACUUM operation
- Used primarily by the query planner for cost estimation
- Provides a snapshot of index structure including entry pages, data pages, and total entries
- The `ginVersion` field indicates the format version of the GIN index
- Essential for monitoring GIN index health and performance characteristics