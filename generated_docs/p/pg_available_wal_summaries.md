# pg_available_wal_summaries

## Location
[src/backend/backup/walsummaryfuncs.c:32-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummaryfuncs.c#L32-L68)

## Overview
A PostgreSQL system function that returns information about available WAL summary files in the pg_wal/summaries directory as a set-returning function.

## Definition


## Detailed Description
This function provides a view of all WAL summary files currently available in the PostgreSQL data directory's pg_wal/summaries subdirectory. It returns a result set with three columns containing timeline ID, start LSN, and end LSN for each WAL summary file. The function uses the GetWalSummaries() function to retrieve the list of available summary files and formats the results as a materialized set-returning function that can be used in SQL queries.

## Parameters / Member Variables
- No input parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Returns a set of tuples with 3 attributes (NUM_WS_ATTS = 3):
  - Timeline ID (int64)  
  - Start LSN (LSN/XLogRecPtr)
  - End LSN (LSN/XLogRecPtr)

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function)
  - [GetWalSummaries](../G/GetWalSummaries.md) (retrieves list of WAL summary files)
  - [Int64GetDatum](../I/Int64GetDatum.md) (converts int64 to Datum)
  - LSNGetDatum (converts XLogRecPtr to Datum)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates heap tuple)
  - tuplestore_puttuple (stores tuple in result set)
- Data types/structures used:
  - ReturnSetInfo (set-returning function metadata)
  - WalSummaryFile (WAL summary file information structure)
  - NUM_WS_ATTS (constant defining number of attributes = 3)
- Called from:
  - Available as SQL system function (no direct code references found)

## Notes and Other Information
- This function is typically exposed as a SQL system function that can be called from SQL queries
- Uses materialized set-returning function pattern for efficient result set handling
- Includes CHECK_FOR_INTERRUPTS() to allow query cancellation during processing
- The function iterates through all available WAL summary files without filtering parameters
- WAL summary files contain metadata about WAL segments and are used for incremental backup operations