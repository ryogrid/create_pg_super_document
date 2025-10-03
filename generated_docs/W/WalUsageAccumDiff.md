# WalUsageAccumDiff

## Location
[src/backend/executor/instrument.c:286-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L286-L291)

## Overview
A utility function that accumulates Write-Ahead Log (WAL) usage statistics by computing the difference between two WalUsage snapshots and adding that difference to a destination WalUsage structure.

## Definition

```c
void
WalUsageAccumDiff(WalUsage *dst, const WalUsage *add, const WalUsage *sub)
```
## Detailed Description
WalUsageAccumDiff is a public function that calculates the incremental WAL usage statistics between two points in time and accumulates those differences into a destination structure. It performs the operation dst += (add - sub) for all WAL usage counters.

This function is essential for PostgreSQL's query instrumentation system, allowing the database to track how much WAL activity occurred during specific operations or time periods. Since WalUsage counters are monotonically increasing and never reset, this difference-based approach enables precise measurement of WAL resource consumption for individual query operations.

The function handles all three WAL metrics: the number of WAL records produced, the number of full page images, and the total size in bytes of WAL records generated during the measured period.

## Parameters / Member Variables
- `*dst`: Pointer to the destination WalUsage structure that will accumulate the computed differences
- `*add`: Pointer to the WalUsage structure representing the ending state (higher counter values)
- `*sub`: Pointer to the WalUsage structure representing the starting state (lower counter values)
## Dependencies
- Functions called/Symbols referenced:
  - [WalUsage](WalUsage.md) (struct type definition)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [InstrStopNode](../I/InstrStopNode.md)
  - [InstrEndParallelQuery](../I/InstrEndParallelQuery.md)
  - [pgstat_flush_wal](../p/pgstat_flush_wal.md)

## Notes and Other Information
- This is a public function (non-static), making it accessible from other compilation units
- The function is crucial for PostgreSQL's instrumentation infrastructure to show WAL usage statistics
- Used extensively in query execution instrumentation and vacuum operations
- The difference calculation assumes that 'add' contains values greater than or equal to 'sub'
- Essential for parallel query execution where WAL statistics from multiple workers need to be aggregated
- Tracks only WAL activity that can be meaningfully measured per query, not global WAL activities
- Located in src/backend/executor/instrument.c:286-291

## Simplified Source

```c
// Simplified version of WalUsageAccumDiff
void WalUsageAccumDiff(WalUsage *dst, const WalUsage *add, const WalUsage *sub) {
    // Accumulate difference in WAL bytes written
    dst->wal_bytes += add->wal_bytes - sub->wal_bytes;

    // Accumulate difference in WAL records written
    dst->wal_records += add->wal_records - sub->wal_records;

    // Accumulate difference in full page images written
    dst->wal_fpi += add->wal_fpi - sub->wal_fpi;
}
```

Key simplifications made:
- Added explanatory comments for each counter accumulation
- Function is already very simple, so minimal changes needed
- Preserved the core arithmetic operation: dst += (add - sub)