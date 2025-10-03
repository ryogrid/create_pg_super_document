# trackitem_compare_element

## Location
[src/backend/utils/adt/array_typanalyze.c:768-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_typanalyze.c#L768-L779)

## Overview
A static comparison function used to sort TrackItem pointers by their element values using the underlying data type's comparison function.

## Definition

```c
static int
trackitem_compare_element(const void *e1, const void *e2, void *arg)
```
## Detailed Description
This function implements a comparator for sorting TrackItem structures based on their key/element values rather than their frequencies. It serves as a wrapper around the element_compare function, extracting the key values from TrackItem structures and delegating the actual comparison to the type-specific comparison logic. This function is essential for organizing array statistics data by element value order, which is useful for creating sorted most common values (MCV) lists and for binary search operations on statistical data.

The function follows the standard C library qsort comparison interface and is specifically used in PostgreSQL's array type statistics collection and analysis.

## Parameters / Member Variables
- `*e1`: Pointer to the first TrackItem pointer to compare (cast from const void*)
- `*e2`: Pointer to the second TrackItem pointer to compare (cast from const void*)
- `*arg`: Unused context argument (required by qsort interface but not used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [element_compare](../e/element_compare.md) (performs the actual element value comparison)
  - [TrackItem](../T/TrackItem.md) (structure containing the key values being compared)
- Called from (representative examples):
  - [compute_array_stats](../c/compute_array_stats.md) (for sorting array element statistics by value)

## Notes and Other Information
- Returns the same comparison values as element_compare: <0, 0, >0 for less than, equal, greater than
- Operates on pointers to TrackItem pointers, requiring double dereferencing to access key values
- Part of PostgreSQL's array statistics collection system for query optimization
- Used to create ordered MCV (Most Common Values) lists for selectivity estimation
- The actual comparison logic is delegated to element_compare, which uses the element type's btree comparison operator
- Critical for maintaining sorted order of statistical data used by the query planner