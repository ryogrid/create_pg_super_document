# dump_one_relation

## Location
[src/bin/pg_walsummary/pg_walsummary.c:129-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_walsummary/pg_walsummary.c#L129-L218)

## Overview
Dumps detailed block information for a single relation (table or index) from WAL summary data, handling memory allocation and formatting output in ranges.

## Definition

```c
static void
dump_one_relation(ws_options *opt, RelFileLocator *rlocator,
				  ForkNumber forknum, BlockNumber limit_block,
				  BlockRefTableReader *reader)
```
## Detailed Description
This function extracts and displays block reference information for a specific relation from WAL summary data. It dynamically allocates and resizes a buffer to hold block numbers, sorts them for proper display, and formats the output as either individual blocks or block ranges depending on user options. The function handles memory management efficiently by doubling buffer size when needed and includes overflow protection. When not in quiet mode, it outputs formatted information showing tablespace, database, relation, fork, and block details.

## Parameters / Member Variables
- `*opt`: Options structure containing display preferences (quiet mode, individual block display)
- `*rlocator`: Relation file locator containing tablespace OID, database OID, and relation number
- `forknum`: Fork number identifying which fork of the relation (main, FSM, VM, etc.)
- `limit_block`: Block number limit for the relation, or InvalidBlockNumber if none
- `*reader`: Block reference table reader for extracting block information
## Dependencies
- Functions called/Symbols referenced:
  - palloc_array (memory allocation)
  - repalloc_array (memory reallocation)
  - [BlockRefTableReaderGetBlocks](../B/BlockRefTableReaderGetBlocks.md) (block data extraction)
  - qsort (block number sorting)
  - [compare_block_numbers](../c/compare_block_numbers.md) (comparison function for sorting)
  - printf (output formatting)
- Called from:
  - [main](../m/main.md) function in pg_walsummary.c:116

## Notes and Other Information
- Uses a global block_buffer that is allocated on first use and reused across calls
- Implements dynamic buffer resizing with overflow protection using PG_UINT32_MAX
- Sorts block numbers to handle cases where array-of-offsets representation returns unsorted data
- Supports both individual block and block range output modes
- Outputs are formatted with tablespace, database, relation, and fork identifiers for clarity
- Function is static and only used within the pg_walsummary utility