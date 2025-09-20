# array_ne

## Location
[src/backend/utils/adt/arrayfuncs.c:3931-3936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3931-L3936)

## Overview
Implements array inequality comparison by negating the result of the array equality function, providing the "not equal" operator for PostgreSQL arrays.

## Definition

```c
Datum
array_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a simple wrapper around the  function that implements the "not equal" operator (<>) for PostgreSQL arrays. Rather than duplicating the complex array comparison logic, it leverages the existing equality implementation and simply negates the result.

This function is part of PostgreSQL's array-array boolean operator family, which provides element-by-element comparison operations similar to text comparison functions but operating on array elements instead of characters. The design follows PostgreSQL's convention of implementing inequality operators as negations of equality operators when possible, ensuring consistency and reducing code duplication.

The function delegates all the actual comparison work to , including dimension checking, element type validation, null handling, and element-by-element comparison using the appropriate equality operator for the element type.

## Parameters / Member Variables
- Function receives two array arguments via  macro:
  - : First array to compare (argument 0, passed through to array_eq)
  - : Second array to compare (argument 1, passed through to array_eq)

## Dependencies
- Functions called/Symbols referenced:
  - [array_eq](array_eq.md) (the core array equality function)
  - [DatumGetBool](../D/DatumGetBool.md) (extracts boolean value from Datum)
  - PG_RETURN_BOOL (returns boolean result)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Extremely lightweight implementation - just a logical negation of array_eq
- Inherits all the sophisticated comparison logic from array_eq including:
  - Fast-path dimension and bounds checking
  - Element type validation
  - Null value handling (NULL = NULL is false for inequality)
  - Collation-aware element comparison
  - Memory management for toasted arrays
- Part of the array-array boolean operator family in PostgreSQL
- Follows the same error handling patterns as array_eq
- Performance characteristics are identical to array_eq since it simply negates the result
- Used through PostgreSQL's operator system for expressions like 
- Maintains consistency with SQL standards for array inequality operations
- The function comment indicates it's part of a broader family of array comparison operators that use element-by-element iteration logic