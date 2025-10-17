# _PrintTocData

## Location
[src/bin/pg_dump/pg_backup_custom.c:416-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L416-L568)

## Overview
This function prints data for a given table of contents (TOC) entry during PostgreSQL archive restoration, handling both seekable and non-seekable input streams efficiently.

## Definition
```c
static void _PrintTocData(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
_PrintTocData is a core function in the custom archive format that handles the restoration of data for a specific table of contents entry. It implements sophisticated logic to locate and read data blocks from the archive, handling both seekable files (which allow random access) and non-seekable streams (which require sequential scanning). The function can skip over unneeded blocks while remembering their positions for potential future access, and handles both regular data blocks (BLK_DATA) and large object blocks (BLK_BLOBS). It includes robust error handling for missing or corrupt data blocks and maintains position tracking for performance optimization.

## Parameters / Member Variables
- `AH`: Archive handle containing the archive state, file handle, and format-specific data
- `te`: Table of contents entry specifying which data block to restore

## Dependencies
- Functions called/Symbols referenced:
  - [_getFilePos](../g/_getFilePos.md)
  - [_readBlockHeader](../r/_readBlockHeader.md)  
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md)
  - [_skipData](../s/_skipData.md)
  - [_skipLOs](../s/_skipLOs.md)
  - [_PrintData](_PrintData.md)
  - [_LoadLOs](../L/_LoadLOs.md)
  - fseeko
  - pg_log_warning
  - [pg_fatal](../p/pg_fatal.md)
  - [TocEntry](../T/TocEntry.md) (type)
  - [lclContext](../l/lclContext.md) (type)
  - lclTocEntry (type)
  - Constants: K_OFFSET_NO_DATA, K_OFFSET_POS_NOT_SET, K_OFFSET_POS_SET, BLK_DATA, BLK_BLOBS
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (registered as callback)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (referenced in directory format)
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md) (referenced in null format)

## Notes and Other Information
This function includes special handling for Windows threading concerns where multiple threads might access the same lclTocEntry concurrently. The function implements a block caching mechanism to remember positions of data blocks encountered during sequential scans, improving performance for subsequent random access operations. The function handles both regular table data and large objects through different code paths, ensuring appropriate restoration logic for each data type.

## Simplified Source
```c
static void _PrintTocData(ArchiveHandle *AH, TocEntry *te)
{
    lclContext *ctx = (lclContext *) AH->formatData;
    lclTocEntry *tctx = (lclTocEntry *) te->formatData;
    int blkType, id;

    // Skip if no data to restore
    if (tctx->dataState == K_OFFSET_NO_DATA)
        return;

    // Handle non-seekable files or unknown positions - scan sequentially
    if (!ctx->hasSeek || tctx->dataState == K_OFFSET_POS_NOT_SET) {
        // Start from last known position if seekable
        if (ctx->hasSeek)
            fseeko(AH->FH, ctx->lastFilePos, SEEK_SET);

        // Search for the target block
        for (;;) {
            pgoff_t thisBlkPos = _getFilePos(AH, ctx);
            _readBlockHeader(AH, &blkType, &id);

            if (blkType == EOF || id == te->dumpId)
                break;

            // Cache block positions for future use
            if (thisBlkPos >= 0) {
                TocEntry *otherte = getTocEntryByDumpId(AH, id);
                if (otherte && otherte->formatData) {
                    lclTocEntry *othertctx = (lclTocEntry *) otherte->formatData;
                    if (othertctx->dataState == K_OFFSET_POS_NOT_SET) {
                        othertctx->dataPos = thisBlkPos;
                        othertctx->dataState = K_OFFSET_POS_SET;
                    }
                }
            }

            // Skip over unwanted blocks
            switch (blkType) {
                case BLK_DATA:
                    _skipData(AH);
                    break;
                case BLK_BLOBS:
                    _skipLOs(AH);
                    break;
                default:
                    pg_fatal("unrecognized data block type (%d)", blkType);
            }
        }
    } else {
        // Direct seek to known position
        fseeko(AH->FH, tctx->dataPos, SEEK_SET);
        _readBlockHeader(AH, &blkType, &id);
    }

    // Verify we found the correct block
    if (blkType == EOF)
        pg_fatal("could not find block ID %d in archive", te->dumpId);
    if (id != te->dumpId)
        pg_fatal("found unexpected block ID (%d) when reading data", id);

    // Process the data block
    switch (blkType) {
        case BLK_DATA:
            _PrintData(AH);
            break;
        case BLK_BLOBS:
            _LoadLOs(AH, AH->public.ropt->dropSchema);
            break;
        default:
            pg_fatal("unrecognized data block type %d", blkType);
    }

    // Update search position for future operations
    if (ctx->hasSeek && tctx->dataState == K_OFFSET_POS_NOT_SET) {
        pgoff_t curPos = _getFilePos(AH, ctx);
        if (curPos > ctx->lastFilePos)
            ctx->lastFilePos = curPos;
    }
}
```