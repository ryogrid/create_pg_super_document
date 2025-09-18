# datasegpath

## Location
[src/bin/pg_rewind/filemap.c:653-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L653-L679)

## Overview
Creates the file path for a PostgreSQL relation data file segment by combining the base relation path with an optional segment number.

## Definition
static char *datasegpath(RelFileLocator rlocator, ForkNumber forknum, BlockNumber segno)

## Detailed Description
This helper function constructs the complete file path for a PostgreSQL relation data file, handling both single-file relations and multi-segment relations. It works by:

1. First calling relpathperm() to get the base path for the relation file based on the RelFileLocator and fork number
2. If the segment number (segno) is greater than 0, it appends the segment number to the base path using the format "<base_path>.<segment_number>"
3. If the segment number is 0 (indicating the first/main segment), it returns the base path without modification

Large PostgreSQL relations are automatically split into multiple segments when they exceed a certain size (typically 1GB). This function handles the path generation for both the main segment and any additional segments.

The returned path is allocated using palloc and must be freed by the caller using pfree().

## Parameters / Member Variables
- rlocator: RelFileLocator structure identifying the tablespace, database, and relation
- forknum: The fork number (e.g., MAIN_FORKNUM, FSM_FORKNUM, VM_FORKNUM)  
- segno: The segment number (0 for main segment, >0 for additional segments)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (struct type)
  - [ForkNumber](../F/ForkNumber.md) (enum type)
  - BlockNumber (typedef)
  - relpathperm
  - [psprintf](../p/psprintf.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [process_target_wal_block_change](../p/process_target_wal_block_change.md)
  - [isRelDataFile](../i/isRelDataFile.md)

## Notes and Other Information
- This is a static function, only visible within the filemap.c compilation unit
- The returned path is palloc'd memory and must be freed by the caller
- Handles the PostgreSQL convention of segmenting large relation files
- Used primarily in pg_rewind for path construction and validation
- Works with any fork type, though pg_rewind primarily uses it with the main fork
- Part of PostgreSQL's file organization system where relations can span multiple physical files