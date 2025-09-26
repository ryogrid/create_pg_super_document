# LogicalTapeSetBlocks

## Location
[src/backend/utils/sort/logtape.c:1181-1184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L1181-L1184)

## Overview
Returns the total disk space currently used by a LogicalTapeSet, measured in blocks, excluding any open write buffers.

## Definition
```c
int64 LogicalTapeSetBlocks(LogicalTapeSet *lts);
```

## Detailed Description
LogicalTapeSetBlocks provides a way to measure the actual disk space consumption of a logical tape set. The function calculates the used disk space by subtracting the number of hole blocks from the total number of blocks written to the underlying file.

The calculation performed is: `nBlocksWritten - nHoleBlocks`, where:
- `nBlocksWritten` represents the total number of blocks written to the underlying BufFile
- `nHoleBlocks` represents unused hole blocks between worker spaces following BufFile concatenation

This function is particularly useful for resource tracking and monitoring during external sorting operations, as it provides an accurate measure of the actual disk space being consumed by the tape set, excluding any fragmentation or unused space.

Note that this function does not account for open write buffers that may contain data not yet flushed to disk.

## Parameters
- `lts`: Pointer to the LogicalTapeSet for which to calculate disk usage

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTapeSet](LogicalTapeSet.md) (struct type)

- Called from (representative examples):
  - [tuplesort_updatemax](../t/tuplesort_updatemax.md) (in tuplesort.c:1005)
  - [tuplesort_free](../t/tuplesort_free.md) (in tuplesort.c:911)  
  - [hash_agg_update_metrics](../h/hash_agg_update_metrics.md) (in nodeAgg.c:1947)

## Notes and Other Information
- The return value is in units of PostgreSQL blocks (BLCKSZ), not bytes
- This function is commonly used in resource tracking contexts, particularly during external sorting operations
- The result excludes allocated but not yet written blocks, providing a conservative estimate of actual disk usage
- In parallel sorting scenarios, this helps track the disk space consumed by worker processes
- The function performs a simple arithmetic operation and is therefore lightweight and suitable for frequent calls during resource monitoring