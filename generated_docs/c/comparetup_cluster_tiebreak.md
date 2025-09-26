# comparetup_cluster_tiebreak

## Location
[src/backend/utils/sort/tuplesortvariants.c:1248-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1248-L1354)

## Overview
Performs comprehensive multi-column comparison of cluster tuples, handling both simple attribute-based indexes and complex expression-based indexes for complete tuple ordering.

## Definition
```c
static int comparetup_cluster_tiebreak(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
```

## Detailed Description
The `comparetup_cluster_tiebreak` function implements the comprehensive comparison logic for CLUSTER operations when either the leading sort key comparison results in equality or when no cached datum is available. It handles two distinct scenarios:

1. **Simple Attribute Indexes**: For regular indexes based on table columns, it iterates through all index attributes, extracting values using `heap_getattr` and comparing them using appropriate sort comparators.

2. **Expression Indexes**: For indexes based on expressions or functions, it computes the complete index tuple values using `FormIndexDatum` and then compares the computed values. This requires setting up an expression context and may involve complex expression evaluation.

The function also handles abbreviation comparators for the leading sort key when available, providing optimized comparison for abbreviated keys while falling back to full comparison when necessary.

## Parameters / Member Variables
- `a`: First SortTuple to compare
- `b`: Second SortTuple to compare
- `state`: Tuplesortstate containing sorting context and configuration

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - heap_getattr
  - ApplySortAbbrevFullComparator
  - ApplySortComparator
  - ResetPerTupleExprContext
  - GetPerTupleExprContext
  - ExecStoreHeapTuple
  - FormIndexDatum
  - INDEX_MAX_KEYS
  - TuplesortClusterArg
- Called from (representative examples):
  - comparetup_cluster
  - tuplesort_begin_cluster
  - CLUSTER_SORT operations

## Notes and Other Information
- Returns negative, zero, or positive integer indicating the relative ordering of the tuples
- Handles both simple column-based indexes and complex expression-based indexes
- For expression indexes, memory context is reset between comparisons to prevent memory leaks
- The function optimizes by starting comparison from the second key when the first key has already been compared
- Uses INDEX_MAX_KEYS arrays to store computed index values for expression-based comparisons
- Part of PostgreSQL's CLUSTER implementation that physically reorganizes table data according to index ordering
- The tiebreak mechanism ensures stable and complete ordering even for complex multi-column indexes with expressions