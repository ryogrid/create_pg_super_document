# table_scan_bitmap_next_block

## Location
src/include/access/tableam.h: 1962 - 1985

## Overview
Prepares to fetch, check, or return tuples from a specific block as part of a bitmap table scan operation.

## Definition
```c
static inline bool
table_scan_bitmap_next_block(TableScanDesc scan,
                            struct TBMIterateResult *tbmres)
```

## Detailed Description
This function is part of PostgreSQL's table access method interface and serves as a preparation step for bitmap heap scans. It checks whether there are tuples to be found on a specific page (identified by `tbmres->blockno`) and returns a boolean indicating the result. 

The function is an optional table access method implementation that should only be used after verifying its presence at plan time. It acts as a wrapper that delegates to the specific table access method's `scan_bitmap_next_block` implementation through the `rd_tableam` interface.

The function includes a safety check to prevent unexpected calls during logical decoding operations, which could cause issues with transaction consistency.

## Parameters / Member Variables
- `scan`: TableScanDesc - The scan descriptor that must have been initialized via `table_beginscan_bm()`
- `tbmres`: struct TBMIterateResult - Contains the block number and other bitmap iteration results for the block to be processed

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`: Checks if a transaction ID is valid
  - `CheckXidAlive`: Global variable used during logical decoding
  - `bsysscan`: Boolean flag indicating system catalog scan
  - `elog`: Error logging function
  - `scan->rs_rd->rd_tableam->scan_bitmap_next_block`: Actual table access method implementation

- Called from (representative examples):
  - `BitmapHeapNext`: Main function in bitmap heap scan executor node

## Notes and Other Information
- This is an inline function defined in the table access method header file
- The function is optional and implementations should verify its availability before use
- Contains specific safety checks for logical decoding scenarios
- Returns `false` if no tuples are found on the page, `true` otherwise
- Part of the bitmap scan optimization pathway in PostgreSQL's executor