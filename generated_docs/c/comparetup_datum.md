# comparetup_datum

## Location
[src/backend/utils/sort/tuplesortvariants.c:1794-1808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1794-L1808)

## Overview
Compares two SortTuple structures containing datum values for sorting operations, serving as the primary comparison function for datum-based tuple sorting.

## Definition

```c
static int
comparetup_datum(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
```
## Detailed Description
This function performs comparison of two datum-based tuples during sorting operations. It first applies the primary sort comparator using the datum1 field and isnull1 flag from each SortTuple. If the primary comparison yields equality (compare == 0), it delegates to comparetup_datum_tiebreak() to resolve the tie using secondary criteria. This function is specifically designed for sorting single datum values rather than full tuples, making it efficient for operations like ORDER BY on single columns.

## Parameters / Member Variables
- : Pointer to the first SortTuple to compare, containing datum1 and isnull1 fields
- : Pointer to the second SortTuple to compare, containing datum1 and isnull1 fields  
- : Pointer to the Tuplesortstate containing sort configuration and context

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - [comparetup_datum_tiebreak](comparetup_datum_tiebreak.md)
- Called from (representative examples):
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md)
  - CLUSTER_SORT

## Notes and Other Information
This function is part of the tuple sorting framework and is registered as a comparison callback for datum-based sorts. The two-stage comparison (primary + tiebreak) ensures stable sorting behavior when primary keys are equal. The function operates on the datum1/isnull1 fields of SortTuple rather than full tuple data, making it more efficient for single-value sorting scenarios.

## Simplified Source

```c
static int comparetup_datum(const SortTuple *a, const SortTuple *b, Tuplesortstate *state) {
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    int compare;

    // Compare primary datum values
    compare = ApplySortComparator(a->datum1, a->isnull1,
                                  b->datum1, b->isnull1,
                                  base->sortKeys);

    // If equal, use tiebreaker comparison
    if (compare != 0)
        return compare;

    return comparetup_datum_tiebreak(a, b, state);
}
```