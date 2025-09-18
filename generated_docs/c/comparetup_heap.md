# comparetup_heap

## Location
src/backend/utils/sort/tuplesortvariants.c: 1085 - 1103

## Overview
A specialized static comparison function for heap tuples that implements the primary comparison logic for sorting HeapTuple/MinimalTuple data structures.

## Definition
```c
static int comparetup_heap(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
```

## Detailed Description
This function serves as the primary tuple comparison routine for heap tuple sorting operations within PostgreSQL's tuplesort framework. It implements a two-stage comparison strategy: first comparing the leading sort key using the optimized ApplySortComparator function, and then delegating to comparetup_heap_tiebreak for additional sort keys if the primary comparison yields equality.

The function is designed to work efficiently with the SortTuple abstraction, which contains cached datum1 values (potentially abbreviated) for the primary sort key. This design allows for fast comparison of the most significant sort column without requiring expensive attribute extraction in most cases.

When the primary sort key comparison returns non-zero (indicating the tuples are definitively ordered), the function returns immediately. Only when the primary keys are equal does it proceed to the more expensive tiebreak comparison that may involve extracting and comparing additional attributes.

## Parameters / Member Variables
- `a`: Pointer to the first SortTuple to compare
- `b`: Pointer to the second SortTuple to compare  
- `state`: Tuplesortstate containing sorting context and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - ApplySortComparator
  - comparetup_heap_tiebreak
  - SortSupport (type)
  - TuplesortPublic (struct type)
- Called from (representative examples):
  - tuplesort_begin_heap (via CLUSTER_SORT macro)

## Notes and Other Information
- This is a static function, only accessible within the tuplesortvariants.c file
- Returns integer comparison result: negative if a < b, zero if a == b, positive if a > b
- Optimized for performance by checking the primary sort key first before expensive tiebreak operations
- Part of the heap tuple sorting specialization within the broader tuplesort framework
- Works with both full and abbreviated sort keys, depending on the sorting configuration
- The CLUSTER_SORT macro references this function, indicating its use in table clustering operations
- Designed to handle NULL values properly through the ApplySortComparator interface