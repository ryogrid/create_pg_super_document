# table_scan_sample_next_tuple

## Location
[src/include/access/tableam.h:2035-2110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L2035-L2110)

## Overview
Fetches the next sample tuple into a slot during a TABLESAMPLE scan, coordinating with the sampling method's tuple selection logic.

## Definition
```c
static inline bool
table_scan_sample_next_tuple(TableScanDesc scan,
                            struct SampleScanState *scanstate,
                            TupleTableSlot *slot)
```

## Detailed Description
This function is the core tuple-fetching mechanism for PostgreSQL's TABLESAMPLE feature. It retrieves the next tuple from the current block and determines whether it should be included in the sample according to the sampling method's logic. The function works in conjunction with `table_scan_sample_next_block()` which must have previously selected a valid block.

Unlike regular table scans, this function delegates the tuple selection decision to the sampling method through the `TsmRoutine->NextSampleTuple()` callback. This allows different sampling algorithms (such as SYSTEM for block-level sampling or BERNOULLI for tuple-level sampling) to implement their own inclusion criteria while maintaining a consistent interface.

The function operates under the same preconditions as other tuple-fetching functions: the block must have been selected by a prior call to `table_scan_sample_next_block()`, and no previous `table_scan_sample_next_tuple()` call for the same block should have returned `false`.

## Parameters / Member Variables
- `scan`: TableScanDesc - The active sample scan descriptor
- `scanstate`: struct SampleScanState - The sampling method state containing configuration and random number generation state
- `slot`: TupleTableSlot - The destination slot where the selected sample tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`: Transaction ID validation
  - `CheckXidAlive`: Global variable for logical decoding safety
  - `bsysscan`: System catalog scan indicator
  - `elog`: Error logging mechanism
  - `scan->rs_rd->rd_tableam->scan_sample_next_tuple`: Table access method implementation
  - `TsmRoutine->NextSampleTuple`: Sampling method callback (mentioned in comments)

- Called from (representative examples):
  - `tablesample_getnext`: Main sample scan executor function

## Notes and Other Information
- Must be called after `table_scan_sample_next_block()` has successfully selected a block
- Returns `true` if a visible tuple was selected for the sample, `false` otherwise
- The actual sampling decision is made by the pluggable sampling method via `TsmRoutine->NextSampleTuple()`
- Different sampling methods can implement different strategies (block-level vs tuple-level sampling)
- Contains logical decoding safety checks consistent with other table scan functions
- Part of PostgreSQL's statistical sampling infrastructure used for query optimization and analytical workloads
- Proper sequencing with block selection is critical for correct sampling behavior