# WalUsageAdd

## Location
[src/backend/executor/instrument.c:278-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L278-L285)

## Overview
A static helper function that accumulates Write-Ahead Log (WAL) usage statistics by adding all fields from a source WalUsage structure to a destination WalUsage structure.

## Definition

```c
static void
WalUsageAdd(WalUsage *dst, WalUsage *add)
```
## Detailed Description
WalUsageAdd is a utility function used to aggregate WAL usage statistics in PostgreSQL's instrumentation system. It performs element-wise addition of all WAL usage counters from the  parameter to the  parameter. This function is essential for accumulating WAL statistics across multiple operations or parallel workers.

The function tracks three key WAL metrics:
1. **wal_records**: The number of WAL records produced
2. **wal_fpi**: The number of WAL full page images produced  
3. **wal_bytes**: The total size in bytes of WAL records produced

This function focuses specifically on WAL activity that can be meaningfully measured per query, such as record generation, rather than global WAL activities like WAL writes which are tracked separately by WAL global statistics counters.

## Parameters / Member Variables
- : Pointer to the destination WalUsage structure that will receive the accumulated values
- : Pointer to the source WalUsage structure whose values will be added to dst

## Dependencies
- Functions called/Symbols referenced:
  - WalUsage (struct type definition)
- Called from (representative examples):
  - [InstrAggNode](../I/InstrAggNode.md)
  - [InstrAccumParallelQuery](../I/InstrAccumParallelQuery.md)

## Notes and Other Information
- This is a static function, so it's only accessible within the same compilation unit (instrument.c)
- The function performs simple accumulation without any validation or overflow checking
- All WalUsage counters are designed to be monotonically increasing and never reset to zero
- The function is crucial for PostgreSQL's query execution instrumentation, particularly for EXPLAIN output and pg_stat_statements
- WalUsage tracking is separate from general WAL statistics and focuses on per-query measurable activity
- Located in src/backend/executor/instrument.c:278-285