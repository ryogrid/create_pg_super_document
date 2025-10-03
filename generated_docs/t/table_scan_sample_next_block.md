# table_scan_sample_next_block

## Location
[src/include/access/tableam.h:2013-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L2013-L2034)

## Overview
Prepares to fetch tuples from the next block in a table sample scan, coordinating with TABLESAMPLE sampling methods.

## Definition
```c
static inline bool
table_scan_sample_next_block(TableScanDesc scan,
                            struct SampleScanState *scanstate)
```

## Detailed Description
This function is part of PostgreSQL's TABLESAMPLE implementation, which allows statistical sampling of table data. It advances the sample scan to the next block of data to be processed, working in coordination with the sampling method's logic.

The function can operate in two modes: if the sampling method provides a `NextSampleBlock()` callback (via the `TsmRoutine` interface), it will use that method-specific logic to determine the next block to sample. Otherwise, it performs a sequential scan over the underlying relation, allowing the sampling method to decide which tuples to include during the tuple-fetching phase.

This flexibility allows different sampling methods (like SYSTEM and BERNOULLI) to implement their own block selection strategies while maintaining a consistent interface.

## Parameters / Member Variables
- `scan`: TableScanDesc - The scan descriptor that must have been initialized via `table_beginscan_sampling()`
- `scanstate`: struct SampleScanState - Contains the sampling method state and configuration for the current sample scan

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`: Transaction ID validation function
  - `CheckXidAlive`: Global variable for logical decoding safety
  - `bsysscan`: Boolean indicating system catalog scan
  - `elog`: Error logging function
  - `scan->rs_rd->rd_tableam->scan_sample_next_block`: Table access method implementation
  - `[TsmRoutine](../T/TsmRoutine.md)->NextSampleBlock`: Optional sampling method callback (mentioned in comments)

- Called from (representative examples):
  - `[tablesample_getnext](tablesample_getnext.md)`: Main function in the sample scan executor node

## Notes and Other Information
- Must be preceded by `table_beginscan_sampling()` to initialize the sample scan
- Returns `false` when the sample scan is finished, `true` when more blocks are available
- Supports pluggable sampling methods through the `TsmRoutine` interface
- Falls back to sequential scanning when sampling methods don't provide custom block selection
- Contains the same logical decoding safety checks as other table scan functions
- Part of PostgreSQL's TABLESAMPLE feature which enables statistical sampling for performance and analytical purposes

## Simplified Source

```c
static inline bool table_scan_sample_next_block(TableScanDesc scan,
                                               struct SampleScanState *scanstate) {
    // Safety check: prevent calls during logical decoding for non-system tables
    if (unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan)) {
        elog(ERROR, "unexpected table_scan_sample_next_block call during logical decoding");
    }

    // Delegate to table access method implementation
    // Will use sampling method's NextSampleBlock() if available,
    // otherwise performs sequential scan
    return scan->rs_rd->rd_tableam->scan_sample_next_block(scan, scanstate);
}
```