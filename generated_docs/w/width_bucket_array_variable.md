# width_bucket_array_variable

## Location
[src/backend/utils/adt/arrayfuncs.c:6840-6909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6840-L6909)

## Overview
Implements width_bucket functionality for generic variable-width data types using binary search with optimized array element traversal.

## Definition

```c
static int
width_bucket_array_variable(Datum operand,
							ArrayType *thresholds,
							Oid collation,
							TypeCacheEntry *typentry)
```
## Detailed Description
This function provides width bucketing for variable-width data types (where typlen <= 0, such as text, varchar, bytea). Unlike fixed-width types, variable-width types cannot be accessed through simple pointer arithmetic, requiring sequential traversal to locate specific array elements.

The function implements an optimized binary search that minimizes the cost of variable-width array element access. A key optimization is that after comparing with an element during binary search, if the search moves to the right half, the function advances the thresholds_data pointer to avoid re-traversing the same elements in subsequent iterations. This reduces the overall complexity from O(N^2) to O(N) for array indexing operations.

The function handles the complexities of variable-width data storage, including proper alignment requirements and length calculations using PostgreSQL's attribute access functions.

## Parameters / Member Variables
- : The value to be bucketed (passed as Datum)
- : ArrayType containing sorted threshold values with no NULLs
- : The collation to use for comparison operations
- : TypeCacheEntry containing type information including typlen, typbyval, typalign, and comparison function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO
  - ARR_DATA_PTR
  - InitFunctionCallInfoData
  - ArrayGetNItems
  - ARR_NDIM
  - ARR_DIMS
  - att_addlength_pointer
  - att_align_nominal
  - fetch_att
  - FunctionCallInvoke
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Called from:
  - [width_bucket_array](width_bucket_array.md) (src/backend/utils/adt/arrayfuncs.c:6727)

## Notes and Other Information
- Designed specifically for variable-width types where element length varies
- Uses sequential traversal to locate array elements since direct indexing is not possible
- Implements a crucial optimization: advances the base pointer when moving to the right in binary search to avoid redundant traversal
- Properly handles alignment requirements for variable-width data using att_align_nominal
- Uses att_addlength_pointer to calculate the next element's position based on the current element's length
- Maintains O(log N) search complexity while keeping array access overhead to O(N) instead of O(N^2)
- Static function, only accessible within the same compilation unit
- Returns bucket number ranging from 0 to N (where N is the number of thresholds)