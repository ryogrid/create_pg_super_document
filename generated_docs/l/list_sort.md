# list_sort

## Location
[src/backend/nodes/list.c:1674-1690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1674-L1690)

## Overview
Sorts a PostgreSQL list in-place using a user-provided comparator function, based on the standard library qsort algorithm.

## Definition

```c
typedef int (*qsort_comparator) (const void *a, const void *b);
```
## Detailed Description
The  function sorts a PostgreSQL List structure in-place using a user-provided comparison function. It's a wrapper around the standard library's  function, providing the same O(N log N) time complexity and similar behavior regarding sort stability (no guarantees for equal keys).

The function is designed to work with PostgreSQL's List structure by accepting a comparator function that receives  arguments, allowing the comparator to use  and related macros directly without type casting. The comparator function should return a negative value if the first element is less than the second, zero if they're equal, and a positive value if the first is greater.

The function includes an optimization to skip sorting when the list has fewer than two elements, and validates the list structure before proceeding with the sort operation.

## Parameters / Member Variables
- : The List to sort in-place. Must be a valid List structure (not NIL).
- : A comparator function of type  that takes two  arguments and returns an integer indicating their relative ordering (negative, zero, or positive).

## Dependencies
- Functions called/Symbols referenced:
  - [check_list_invariants](../c/check_list_invariants.md)
  - qsort (standard library function)
  - list_length (indirectly referenced)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [WalSummariesAreComplete](../W/WalSummariesAreComplete.md)
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)
  - [GetPublicationRelations](../G/GetPublicationRelations.md)
  - [create_append_path](../c/create_append_path.md)
  - [expand_grouping_sets](../e/expand_grouping_sets.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)

## Notes and Other Information
- Sorts the list in-place, modifying the original list structure
- Based on standard library qsort(), providing O(N log N) performance
- No stability guarantees for equal elements (like qsort)
- Comparator function receives  arguments for convenient use with  family of macros
- Optimized to skip sorting for lists with 0 or 1 elements
- Commonly used for sorting relation lists, path lists, and other collections that need ordering
- The comparator function should follow standard qsort conventions for return values
- [List](../L/List.md) structure integrity is validated before sorting