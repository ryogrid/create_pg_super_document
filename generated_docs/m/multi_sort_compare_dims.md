# multi_sort_compare_dims

## Location
[src/backend/statistics/extended_stats.c:899-918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L899-L918)

## Overview
Compares two SortItem structures across a specified range of dimensions, providing flexible partial comparison functionality for multi-dimensional sorting operations.

## Definition


## Detailed Description
This function performs a lexicographic comparison between two SortItem structures, but only across a specified range of dimensions from 'start' to 'end' (inclusive). It iterates through the dimension range, applying the appropriate sort comparator for each dimension, and returns as soon as a non-zero comparison result is found. This selective comparison capability is particularly useful in dependency analysis where only a subset of columns needs to be compared to determine ordering relationships.

## Parameters / Member Variables
- : Integer specifying the first dimension index to compare (inclusive)
- : Integer specifying the last dimension index to compare (inclusive)
- : Pointer to the first SortItem structure to compare
- : Pointer to the second SortItem structure to compare
- : MultiSortSupport structure containing sort configuration for all dimensions

## Dependencies
- Functions called/Symbols referenced:
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - [SortItem](../S/SortItem.md) (type)
  - MultiSortSupport (type)
- Called from (representative examples):
  - [dependency_degree](../d/dependency_degree.md) (src/backend/statistics/dependencies.c:306)

## Notes and Other Information
- Provides more flexibility than multi_sort_compare by allowing range-based comparison
- Returns 0 when all dimensions in the range are equal, following standard comparison semantics
- The function assumes start <= end and both indices are valid within the SortItem arrays
- Primarily used in dependency analysis algorithms that need to examine subsets of column combinations
- Essential for determining partial dependencies in PostgreSQL's extended statistics system
- More efficient than multi_sort_compare when only a subset of dimensions needs comparison