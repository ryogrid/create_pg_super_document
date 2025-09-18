# fetch_array_arg_replace_nulls

## Location
src/backend/utils/adt/array_userfuncs.c: 64 - 122

## Overview
A static helper function that fetches an array-valued argument in expanded form, replacing null values with an empty array of the appropriate data type.

## Definition
```c
static ExpandedArrayHeader *fetch_array_arg_replace_nulls(FunctionCallInfo fcinfo, int argno)
```

## Detailed Description
This function is designed to handle array arguments in PostgreSQL functions, particularly those that need to work with both null and non-null array inputs. It fetches an array argument in expanded form, which is an optimized representation for array manipulation. When the input argument is null, instead of returning null, it constructs an empty array of the proper data type.

The function includes memory management optimizations for aggregate functions by ensuring newly-created expanded arrays are allocated in the aggregate state context to minimize copying operations. It also caches element type information in the function's fn_extra field for performance.

A key design consideration is that if the input is a read/write pointer, the function returns the input argument directly, requiring callers to ensure their modifications are safe and won't corrupt the array state.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context and arguments
- `argno`: The argument number (0-based) to fetch from the function call

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc
  - AggCheckCallContext
  - PG_GETARG_EXPANDED_ARRAYX
  - get_fn_expr_argtype
  - get_element_type
  - construct_empty_expanded_array
- Called from (representative examples):
  - array_append
  - array_prepend

## Notes and Other Information
- This is a static function, only accessible within the array_userfuncs.c file
- Uses ArrayMetaState caching mechanism for performance optimization
- Handles both aggregate and non-aggregate function contexts appropriately
- Provides robust error handling for invalid data types
- Critical for array manipulation functions that need to handle null inputs gracefully