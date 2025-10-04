# pg_wal_summary_contents

## Location
[src/backend/backup/walsummaryfuncs.c:69-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummaryfuncs.c#L69-L176)

## Overview
A PostgreSQL system function that reads and returns the detailed contents of a specific WAL summary file, including information about modified blocks for each relation fork.

## Definition

```c
Datum
pg_wal_summary_contents(PG_FUNCTION_ARGS)
```
## Detailed Description
This function reads the contents of a specified WAL summary file identified by timeline ID, start LSN, and end LSN. It parses the summary file using the BlockRefTableReader infrastructure to extract information about modified database blocks. The function returns a result set containing details about each relation fork and the specific blocks that were modified, including both individual block numbers and limit blocks that indicate truncation points. The output includes relation identifiers, fork numbers, block numbers, and flags indicating whether each entry represents a limit block.

## Parameters / Member Variables
- Input parameters:
  - Timeline ID (int64): Timeline identifier for the WAL summary file
  - Start LSN: Starting log sequence number of the summary file  
  - End LSN: Ending log sequence number of the summary file
- Returns a set of tuples with 6 attributes (NUM_SUMMARY_ATTS = 6):

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function)
  - PG_GETARG_INT64, PG_GETARG_LSN (parameter extraction macros)
  - [OpenWalSummaryFile](../O/OpenWalSummaryFile.md) (opens WAL summary file)
  - [CreateBlockRefTableReader](../C/CreateBlockRefTableReader.md) (creates block reference table reader)
  - [ReadWalSummary](../R/ReadWalSummary.md) (callback function for reading WAL summary data)
  - [ReportWalSummaryError](../R/ReportWalSummaryError.md) (error reporting callback)
  - [BlockRefTableReaderNextRelation](../B/BlockRefTableReaderNextRelation.md) (iterates over relations)
  - [BlockRefTableReaderGetBlocks](../B/BlockRefTableReaderGetBlocks.md) (gets modified block numbers)
  - BlockNumberIsValid (validates block numbers)
  - [DestroyBlockRefTableReader](../D/DestroyBlockRefTableReader.md) (cleanup)
  - [FileClose](../F/FileClose.md) (closes file)
  - Various Datum conversion functions (ObjectIdGetDatum, Int16GetDatum, Int64GetDatum, BoolGetDatum)
- Data types/structures used:
  - [WalSummaryFile](../W/WalSummaryFile.md), WalSummaryIO (WAL summary file handling)
  - [BlockRefTableReader](../B/BlockRefTableReader.md) (block reference table reader)
  - [RelFileLocator](../R/RelFileLocator.md) (relation file locator)
  - [ForkNumber](../F/ForkNumber.md) (relation fork identifier)
  - MAX_BLOCKS_PER_CALL (constant for batch processing)
- Called from:
  - Available as SQL system function (no direct code references found)

## Notes and Other Information
- Validates timeline ID to ensure it's within valid range (1 to PG_INT32_MAX)
- Uses batch processing with MAX_BLOCKS_PER_CALL to efficiently read block lists
- Handles both individual modified blocks and limit blocks (truncation points)
- [Limit](../L/Limit.md) blocks are marked with a boolean flag to distinguish them from regular modified blocks
- Includes interrupt checking for query cancellation during long operations
- Performs proper resource cleanup by destroying the reader and closing files
- The function provides detailed visibility into WAL summary file contents for backup and recovery operations

## Simplified Source
```c
Datum pg_wal_summary_contents(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsi;
    Datum values[NUM_SUMMARY_ATTS];
    bool nulls[NUM_SUMMARY_ATTS];
    WalSummaryFile ws;
    WalSummaryIO io;
    BlockRefTableReader *reader;

    // Initialize set-returning function
    InitMaterializedSRF(fcinfo, 0);
    rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    memset(nulls, 0, sizeof(nulls));

    // Validate timeline ID parameter
    int64 raw_tli = PG_GETARG_INT64(0);
    if (raw_tli < 1 || raw_tli > PG_INT32_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid timeline %lld", (long long) raw_tli)));
    }

    // Open WAL summary file
    ws.tli = (TimeLineID) raw_tli;
    ws.start_lsn = PG_GETARG_LSN(1);
    ws.end_lsn = PG_GETARG_LSN(2);
    io.file = OpenWalSummaryFile(&ws, false);
    reader = CreateBlockRefTableReader(ReadWalSummary, &io, FilePathName(io.file),
                                       ReportWalSummaryError, NULL);

    // Process each relation fork
    RelFileLocator rlocator;
    ForkNumber forknum;
    BlockNumber limit_block;

    while (BlockRefTableReaderNextRelation(reader, &rlocator, &forknum, &limit_block)) {
        CHECK_FOR_INTERRUPTS();

        // Set common relation info
        values[0] = ObjectIdGetDatum(rlocator.relNumber);
        values[1] = ObjectIdGetDatum(rlocator.spcOid);
        values[2] = ObjectIdGetDatum(rlocator.dbOid);
        values[3] = Int16GetDatum((int16) forknum);

        // Emit limit block if valid
        if (BlockNumberIsValid(limit_block)) {
            values[4] = Int64GetDatum((int64) limit_block);
            values[5] = BoolGetDatum(true);
            tuplestore_puttuple(rsi->setResult, heap_form_tuple(rsi->setDesc, values, nulls));
        }

        // Process modified blocks in batches
        BlockNumber blocks[MAX_BLOCKS_PER_CALL];
        unsigned nblocks;
        while ((nblocks = BlockRefTableReaderGetBlocks(reader, blocks, MAX_BLOCKS_PER_CALL)) > 0) {
            CHECK_FOR_INTERRUPTS();
            values[5] = BoolGetDatum(false);
            for (unsigned i = 0; i < nblocks; ++i) {
                values[4] = Int64GetDatum((int64) blocks[i]);
                tuplestore_puttuple(rsi->setResult, heap_form_tuple(rsi->setDesc, values, nulls));
            }
        }
    }

    // Cleanup
    DestroyBlockRefTableReader(reader);
    FileClose(io.file);
    return (Datum) 0;
}
```