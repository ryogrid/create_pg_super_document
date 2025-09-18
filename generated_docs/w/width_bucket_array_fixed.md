# width_bucket_array_fixed

## Location
src/backend/utils/adt/arrayfuncs.c: 6785 - 6839

## Overview
Implements width_bucket functionality for generic fixed-width data types using binary search with custom comparison functions.

## Definition


## Detailed Description
This function provides width bucketing for fixed-width data types (where typlen > 0) other than float8. It uses a generic binary search algorithm that relies on the data type's comparison function to determine the correct bucket placement.

The function leverages the fact that fixed-width types can be directly indexed within the array data, making element access efficient through pointer arithmetic. It sets up function call infrastructure to invoke the appropriate comparison function for the specific data type, allowing it to work with any comparable fixed-width type.

The binary search maintains the invariant that all values in [0, left) are less than the operand, and all values in [left, right) are greater than or equal to the operand.

## Parameters / Member Variables
- : The value to be bucketed (passed as Datum)
- : ArrayType containing sorted threshold values with no NULLs
- : The collation to use for comparison operations
- : TypeCacheEntry containing type information and comparison function details

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO
  - ARR_DATA_PTR
  - InitFunctionCallInfoData
  - ArrayGetNItems
  - ARR_NDIM
  - ARR_DIMS
  - fetch_att
  - FunctionCallInvoke
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Called from:
  - [width_bucket_array](width_bucket_array.md) (src/backend/utils/adt/arrayfuncs.c:6724)

## Notes and Other Information
- Optimized for fixed-width types where element size is known at compile time
- Uses direct pointer arithmetic to access array elements efficiently (ptr = thresholds_data + mid * typlen)
- Relies on the type's registered comparison function for ordering decisions
- Sets up local function call info structure to invoke comparison functions properly
- Handles collation-sensitive comparisons when applicable
- Static function, only accessible within the same compilation unit
- Returns bucket number ranging from 0 to N (where N is the number of thresholds)