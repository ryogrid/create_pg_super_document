# cost_tuplesort

## Location
src/backend/optimizer/path/costsize.c: 1884 - 1985

## Overview
Determines and returns the cost of sorting a relation using PostgreSQL's tuplesort algorithm, excluding the cost of reading the input data.

## Definition


## Detailed Description
The function calculates sorting costs using different algorithms depending on data size and available memory:

1. **In-memory sort**: When total data volume is less than sort_mem, performs quicksort requiring t*log2(t) tuple comparisons for t tuples.

2. **Disk-based sort**: When data exceeds sort_mem, uses a tape-style merge algorithm with approximately t*log2(t) comparisons plus disk I/O for writing and reading tuples across multiple merge passes.

3. **Bounded heap sort**: When only the first k result tuples are needed and k tuples fit in sort_mem, uses a heap method requiring about t*log2(k) comparisons.

The function assumes disk traffic is 3/4 sequential and 1/4 random accesses, and charges two operator evaluations per tuple comparison by default.

## Parameters / Member Variables
- : Output parameter for the startup cost of the sort operation
- : Output parameter for the per-tuple cost during sort execution
- : Number of tuples in the relation to be sorted
- : Average tuple width in bytes
- : Extra cost per comparison beyond the default
- : Number of kilobytes of work memory allocated for the sort
- : Bound on number of output tuples; -1 if no bound

## Dependencies
- Functions called/Symbols referenced:
  - relation_byte_size
  - tuplesort_merge_order
  - LOG2
  - Cost (type)
- Called from (representative examples):
  - cost_incremental_sort
  - cost_sort

## Notes and Other Information
- Ensures sort cost is never estimated as zero by setting minimum tuple count to 2.0
- Default comparison cost includes 2.0 * cpu_operator_cost
- Uses logarithmic merge calculations: logM(r) = log(r) / log(M) where M is merge order
- Run cost charges cpu_operator_cost per tuple since Sort nodes have less overhead than most plan nodes
- Critical for query planner's decision-making in choosing between different sort strategies