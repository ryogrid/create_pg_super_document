# comparetup_cluster

## Location
src/backend/utils/sort/tuplesortvariants.c: 1227 - 1247

## Overview
Compares two tuples during cluster sorting operations by first comparing the leading sort key and then delegating to tiebreak comparison for comprehensive ordering.

## Definition
```c
static int comparetup_cluster(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
```

## Detailed Description
The `comparetup_cluster` function implements the primary comparison logic for CLUSTER operations in PostgreSQL's tuple sorting infrastructure. It performs a two-stage comparison process optimized for performance:

1. **Fast Path Comparison**: If the leading sort key datum is available (haveDatum1 is true), it performs a quick comparison using the cached `datum1` values and the appropriate sort comparator function.

2. **Comprehensive Comparison**: If the leading key comparison results in equality (or if no cached datum is available), it delegates to `comparetup_cluster_tiebreak` for a complete multi-column comparison using the full btree index definition.

This approach provides optimal performance for datasets where the first sort column has high selectivity, while ensuring correct ordering for all cases through the tiebreak mechanism.

## Parameters / Member Variables
- `a`: First SortTuple to compare
- `b`: Second SortTuple to compare  
- `state`: Tuplesortstate containing sort context and configuration

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - ApplySortComparator
  - comparetup_cluster_tiebreak
  - SortSupport
  - TuplesortPublic
- Called from (representative examples):
  - tuplesort_begin_cluster
  - CLUSTER_SORT operations

## Notes and Other Information
- Returns negative, zero, or positive integer indicating whether tuple `a` is less than, equal to, or greater than tuple `b`
- The `haveDatum1` flag optimization allows skipping datum extraction when cached values are available
- This function is specifically designed for CLUSTER operations which sort table data according to btree index definitions
- The two-stage comparison design balances performance with correctness - fast for simple cases, comprehensive for complex ones
- Part of PostgreSQL's pluggable tuple sorting architecture that provides specialized comparison functions for different tuple types