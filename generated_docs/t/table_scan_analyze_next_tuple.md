# table_scan_analyze_next_tuple

## Location
[src/include/access/tableam.h:1739-1775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1739-L1775)

## Overview
Iterates over tuples in a selected block during table analysis, finding tuples suitable for sampling and tracking live/dead row counts.

## Definition
```c
static inline bool
table_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                              double *liverows, double *deadrows,
                              TupleTableSlot *slot)
```

## Detailed Description
This function is part of PostgreSQL's table access method interface for statistical analysis operations. It iterates through tuples in a block that was previously selected and prepared by `table_scan_analyze_next_block()`. The function evaluates each tuple's visibility and suitability for sampling based on the transaction isolation context.

When a suitable tuple is found, it is stored in the provided slot and the function returns true. The function also maintains accurate counts of live and dead rows encountered during the scan, which are crucial for generating accurate table statistics.

## Parameters / Member Variables
- `scan`: TableScanDesc - The table scan descriptor initialized for analysis
- `OldestXmin`: TransactionId - The oldest transaction ID that should be considered when determining tuple visibility
- `liverows`: double* - Pointer to counter that gets incremented for each live tuple encountered
- `deadrows`: double* - Pointer to counter that gets incremented for each dead tuple encountered  
- `slot`: TupleTableSlot - Slot where a suitable tuple will be stored when found

## Dependencies
- Functions called/Symbols referenced:
  - scan->rs_rd->rd_tableam->scan_analyze_next_tuple (table access method implementation)
- Types referenced:
  - [TableScanDesc](../T/TableScanDesc.md)
  - TransactionId
  - [TupleTableSlot](../T/TupleTableSlot.md)
- Called from (representative examples):
  - [acquire_sample_rows](../a/acquire_sample_rows.md) (src/backend/commands/analyze.c:1212)

## Notes and Other Information
- Must be called on a block where `table_scan_analyze_next_block()` returned true
- Returns true when a suitable tuple is found and stored in slot, false when no more suitable tuples are available in the current block
- Automatically updates live and dead row counters based on tuple visibility
- Part of the table access method abstraction layer
- When this function returns false, it releases any resources acquired by the corresponding `table_scan_analyze_next_block()` call
- The OldestXmin parameter ensures consistent visibility determination across the analysis operation

## Simplified Source

```c
static inline bool table_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
                                                 double *liverows, double *deadrows,
                                                 TupleTableSlot *slot)
{
    // Delegate to table access method's tuple iteration implementation
    return scan->rs_rd->rd_tableam->scan_analyze_next_tuple(scan, OldestXmin,
                                                            liverows, deadrows, slot);
}
```