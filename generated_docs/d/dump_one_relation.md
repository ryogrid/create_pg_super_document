# dump_one_relation

## Location
src/bin/pg_walsummary/pg_walsummary.c: 129 - 218

## Overview
Dumps detailed block information for a single relation (table or index) from WAL summary data, handling memory allocation and formatting output in ranges.

## Definition


## Detailed Description
This function extracts and displays block reference information for a specific relation from WAL summary data. It dynamically allocates and resizes a buffer to hold block numbers, sorts them for proper display, and formats the output as either individual blocks or block ranges depending on user options. The function handles memory management efficiently by doubling buffer size when needed and includes overflow protection. When not in quiet mode, it outputs formatted information showing tablespace, database, relation, fork, and block details.

## Parameters / Member Variables
- : Options structure containing display preferences (quiet mode, individual block display)
- : Relation file locator containing tablespace OID, database OID, and relation number
- : Fork number identifying which fork of the relation (main, FSM, VM, etc.)
- : Block number limit for the relation, or InvalidBlockNumber if none
- : Block reference table reader for extracting block information

## Dependencies
- Functions called/Symbols referenced:
  - palloc_array (memory allocation)
  - repalloc_array (memory reallocation)
  - BlockRefTableReaderGetBlocks (block data extraction)
  - qsort (block number sorting)
  - compare_block_numbers (comparison function for sorting)
  - printf (output formatting)
- Called from:
  - main function in pg_walsummary.c:116

## Notes and Other Information
- Uses a global block_buffer that is allocated on first use and reused across calls
- Implements dynamic buffer resizing with overflow protection using PG_UINT32_MAX
- Sorts block numbers to handle cases where array-of-offsets representation returns unsorted data
- Supports both individual block and block range output modes
- Outputs are formatted with tablespace, database, relation, and fork identifiers for clarity
- Function is static and only used within the pg_walsummary utility