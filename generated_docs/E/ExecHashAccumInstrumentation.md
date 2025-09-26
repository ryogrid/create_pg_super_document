# ExecHashAccumInstrumentation

## Location
[src/backend/executor/nodeHash.c:2857-2875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L2857-L2875)

## Overview
ExecHashAccumInstrumentation accumulates performance statistics from hash table instances by taking maximum values across successive hash table operations within a single plan node.

## Definition
```c
void ExecHashAccumInstrumentation(HashInstrumentation *instrument, HashJoinTable hashtable)
```

## Detailed Description
This function merges instrumentation data from a hash table instance into an accumulating instrumentation record. It is designed to handle scenarios where multiple hash table instances may be created and destroyed during the execution of a single hash join plan node (such as during rescans or hash table rebuilding).

The function employs a "maximum value" strategy for all statistics, taking the larger of the current accumulated value and the new hash table's value. This approach ensures that the most significant resource usage patterns are captured, even if they occurred across different hash table instances.

The rationale behind taking maximum values is pragmatic: while the largest nbuckets and nbatch values might have occurred in different instances (making the combination somewhat artificial), failing to report peak values could lead to misdiagnosis of performance issues. The function prioritizes capturing the most resource-intensive scenarios over maintaining perfect statistical consistency.

The function accumulates the following key metrics:
- Number of buckets (current and original)
- Number of batches (current and original) 
- Peak memory space usage

## Parameters / Member Variables
- `instrument`: HashInstrumentation pointer to the accumulating instrumentation record that will be updated with maximum values
- `hashtable`: HashJoinTable pointer to the hash table instance whose statistics will be merged into the instrumentation record

## Dependencies
- Functions called/Symbols referenced:
  - Max (macro)
- Types used:
  - HashInstrumentation
  - HashJoinTable
- Called from (representative examples):
  - ExecShutdownHash
  - ExecReScanHashJoin

## Notes and Other Information
- The function assumes the instrument parameter points to a zeroed or previously initialized HashInstrumentation structure
- All statistics use a "maximum" aggregation strategy rather than sum or average, focusing on peak resource usage
- The nbuckets_original and nbatch_original values should theoretically be identical across instances, but the function handles them with the same max logic for consistency
- This function is essential for accurate EXPLAIN ANALYZE output, ensuring that peak performance characteristics are captured even when multiple hash table instances are used
- The space_peak metric tracks the maximum memory footprint achieved by any hash table instance
- The function is called during hash table shutdown and rescan operations to maintain cumulative statistics