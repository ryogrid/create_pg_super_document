# element_compare

## Location
src/backend/utils/adt/array_selfuncs.c: 1165 - 1180

## Overview
Comparison function for PostgreSQL array elements that uses the element type's default btree opclass and collation for ordering operations.

## Definition
```c
static int element_compare(const void *key1, const void *key2, void *arg)
```

## Detailed Description
This function provides a standardized comparison mechanism for array elements by leveraging PostgreSQL's type system infrastructure. It extracts Datum values from the provided pointers, uses the type's cached comparison function information, and performs the comparison using the appropriate collation settings. The function follows the standard comparison contract, returning negative, zero, or positive values for less-than, equal, or greater-than relationships respectively.

The function is designed to work with PostgreSQL's function manager infrastructure and supports collation-sensitive data types by using the type's default collation settings.

## Parameters / Member Variables
- `key1`: Pointer to the first Datum value to compare (cast from const void*)
- `key2`: Pointer to the second Datum value to compare (cast from const void*)
- `arg`: Pointer to TypeCacheEntry containing type-specific comparison and collation information

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall2Coll
  - DatumGetInt32
- Called from (representative examples):
  - find_next_mcelem
  - mcelem_array_selec
  - mcelem_array_contain_overlap_selec
  - mcelem_array_contained_selec
  - DECountItem
  - element_match
  - trackitem_compare_element

## Notes and Other Information
- Uses PostgreSQL's function call infrastructure with collation support
- Compatible with standard qsort/bsearch comparison function signature
- TODO: Consider using SortSupport infrastructure for potential performance improvements
- Critical component for array statistics collection and selectivity estimation
- Handles collation-sensitive types through TypeCacheEntry's typcollation field
- Returns standard comparison result: <0, 0, or >0 for less, equal, greater respectively