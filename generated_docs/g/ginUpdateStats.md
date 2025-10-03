# ginUpdateStats

## Location
[src/backend/access/gin/ginutil.c:650-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L650-L701)

## Overview
Updates statistical data in a GIN index's metadata page with new values, handling WAL logging and critical section management for crash recovery and consistency.

## Definition
```c
void ginUpdateStats(Relation index, const GinStatsData *stats, bool is_build)
```

## Detailed Description
The `ginUpdateStats` function writes updated statistical information to a GIN index's metadata page, replacing the stored statistics with new values from a `GinStatsData` structure. It operates within a critical section for atomicity, acquires an exclusive lock on the metapage, and updates fields like total pages, entry pages, data pages, and entry counts. Notably, it does NOT update `nPendingPages` or `ginVersion` fields. The function also handles WAL (Write-Ahead Logging) for crash recovery when the relation requires WAL logging and is not in build mode. Additionally, it ensures proper page header setup by setting `pd_lower` correctly to prevent metadata loss during page compression.

## Parameters / Member Variables
- `index`: Relation pointer to the GIN index whose statistics should be updated
- `stats`: Pointer to a `GinStatsData` structure containing the new statistical values
- `is_build`: Boolean flag indicating whether this is called during index build (affects WAL logging)

## Dependencies
- Functions called/Symbols referenced:
  - `GinStatsData` (structure containing statistical data to write)
  - [GinMetaPageData](../G/GinMetaPageData.md) (structure representing metadata on the metapage)
  - [ReadBuffer](../R/ReadBuffer.md), `LockBuffer`, `UnlockReleaseBuffer` (buffer management functions)
  - [BufferGetPage](../B/BufferGetPage.md), `MarkBufferDirty` (page access and modification functions)
  - `GinPageGetMeta` (macro to extract metadata from a GIN metapage)
  - `START_CRIT_SECTION`, `END_CRIT_SECTION` (critical section management)
  - `RelationNeedsWAL` (function to check if WAL logging is required)
  - [ginxlogUpdateMeta](ginxlogUpdateMeta.md) (structure for WAL record data)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), `XLogRegisterData`, `XLogRegisterBuffer`, `XLogInsert` (WAL logging functions)
  - [PageSetLSN](../P/PageSetLSN.md) (function to set page log sequence number)
  - `GIN_METAPAGE_BLKNO`, `GIN_EXCLUSIVE` (constants for metapage and lock mode)
- Called from (representative examples):
  - [ginbuild](ginbuild.md) (during index construction)
  - [ginvacuumcleanup](ginvacuumcleanup.md) (during vacuum operations)

## Notes and Other Information
- Acquires an exclusive lock on the metapage to prevent concurrent modifications
- Operates within a critical section to ensure atomicity of the update operation
- Does NOT update `nPendingPages` and `ginVersion` fields - these are managed separately
- Handles WAL logging for crash recovery, but skips WAL during index build for performance
- Sets `pd_lower` correctly to prevent metadata loss during page compression (important for pg_upgrade compatibility)
- Creates a WAL record of type `XLOG_GIN_UPDATE_META_PAGE` when WAL logging is required
- Essential for maintaining accurate index statistics used by the query planner

## Simplified Source

```c
void ginUpdateStats(Relation index, const GinStatsData *stats, bool is_build) {
    // Get exclusive lock on metapage
    Buffer metabuffer = ReadBuffer(index, GIN_METAPAGE_BLKNO);
    LockBuffer(metabuffer, GIN_EXCLUSIVE);
    Page metapage = BufferGetPage(metabuffer);
    GinMetaPageData *metadata = GinPageGetMeta(metapage);

    START_CRIT_SECTION();

    // Update statistics (excluding nPendingPages and ginVersion)
    metadata->nTotalPages = stats->nTotalPages;
    metadata->nEntryPages = stats->nEntryPages;
    metadata->nDataPages = stats->nDataPages;
    metadata->nEntries = stats->nEntries;

    // Set pd_lower to prevent metadata loss during compression
    ((PageHeader) metapage)->pd_lower =
        ((char *) metadata + sizeof(GinMetaPageData)) - (char *) metapage;

    MarkBufferDirty(metabuffer);

    // WAL logging if needed (not during build)
    if (RelationNeedsWAL(index) && !is_build) {
        ginxlogUpdateMeta data;
        data.locator = index->rd_locator;
        data.ntuples = 0;
        data.newRightlink = data.prevTail = InvalidBlockNumber;
        memcpy(&data.metadata, metadata, sizeof(GinMetaPageData));

        XLogBeginInsert();
        XLogRegisterData((char *) &data, sizeof(ginxlogUpdateMeta));
        XLogRegisterBuffer(0, metabuffer, REGBUF_WILL_INIT | REGBUF_STANDARD);

        XLogRecPtr recptr = XLogInsert(RM_GIN_ID, XLOG_GIN_UPDATE_META_PAGE);
        PageSetLSN(metapage, recptr);
    }

    UnlockReleaseBuffer(metabuffer);
    END_CRIT_SECTION();
}
```