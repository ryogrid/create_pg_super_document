# array_eq

## Location
src/backend/utils/adt/arrayfuncs.c: 3802 - 3930

## Overview
Compares two PostgreSQL arrays for complete equality, including dimensions, bounds, and element-by-element comparison using the appropriate equality operator for the element type.

## Definition


## Detailed Description
The  function implements comprehensive array equality comparison for PostgreSQL arrays. It performs a multi-stage comparison process: first checking array metadata (element types, dimensions, dimension sizes, and lower bounds), then performing element-by-element comparison using the appropriate equality operator for the element type.

The function is designed to work with any array element type that has a defined equality operator, making it more general than array comparison functions that require total ordering. It uses PostgreSQL's type cache system to efficiently look up and cache the equality operator for the element type, avoiding repeated operator lookups in scenarios where the function is called multiple times with the same element type.

The function handles null elements specially: two null elements are considered equal, but a null element and a non-null element are not equal. The comparison short-circuits as soon as any inequality is found, making it efficient for arrays that differ early in the comparison process.

## Parameters / Member Variables
- Function receives two array arguments via  macro:
  - : First array to compare (argument 0)
  - : Second array to compare (argument 1)
- Uses collation information from the function call context

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P (extracts array arguments)
  - PG_GET_COLLATION (gets collation for comparison)
  - AARR_NDIM, AARR_DIMS, AARR_LBOUND (array metadata access macros)
  - AARR_ELEMTYPE (gets element type)
  - [lookup_type_cache](../l/lookup_type_cache.md) (looks up type information and operators)
  - TYPECACHE_EQ_OPR_FINFO (type cache flag for equality operator)
  - InitFunctionCallInfoData (initializes function call structure)
  - ArrayGetNItems (calculates total number of elements)
  - array_iter_setup, array_iter_next (array iteration functions)
  - FunctionCallInvoke (invokes the equality operator)
  - AARR_FREE_IF_COPY (memory cleanup for toasted arrays)
  - LOCAL_FCINFO (local function call info structure)
  - AnyArrayType (generalized array type)

- Called from (representative examples):
  - [array_ne](array_ne.md) (array inequality function uses this as basis)

## Notes and Other Information
- Does not use array_cmp for comparison, since equality can be meaningful for types without total ordering
- Implements fast-path optimization: returns false immediately if array metadata differs (dimensions, sizes, bounds)
- Uses function info extra space to cache TypeCacheEntry across multiple calls for performance
- Performs element type validation and throws error for arrays with different element types
- Handles memory management by freeing toasted input arrays to prevent memory leaks
- NULL handling follows SQL semantics: NULL = NULL is true, NULL = not-NULL is false
- Short-circuits on first inequality found, making it efficient for dissimilar arrays
- Uses collation-aware comparison when the element type requires it
- Returns a boolean Datum value using PG_RETURN_BOOL macro
- Throws specific errors for unsupported element types (no equality operator) and type mismatches
- Critical function for array indexing, joins, and WHERE clause operations involving arrays