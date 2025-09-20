# array_out

## Location
[src/backend/utils/adt/arrayfuncs.c:1016-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1016-L1200)

## Overview
Converts the internal representation of a PostgreSQL array to its external string format for output and display purposes.

## Definition

```c
struct the output string */
	retval = (char *) palloc(overall_length);
```
## Detailed Description
array_out is the primary output function for PostgreSQL arrays, responsible for converting internal ArrayType structures into their textual representation. The function handles multi-dimensional arrays, properly formats null values, manages element quoting requirements, and includes explicit dimension bounds when necessary. It uses a caching mechanism (ArrayMetaState) to avoid repeated lookups of element type information across function calls.

The function performs several key operations: determines if explicit dimension bounds are needed (when lower bounds aren't 1), converts each array element to string format using the element type's output function, applies proper quoting and escaping for special characters, and constructs the final string with appropriate braces and delimiters. The output format follows PostgreSQL's standard array syntax with curly braces and comma separation.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro - the array is retrieved via PG_GETARG_ANY_ARRAY_P(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P
  - AARR_ELEMTYPE
  - AARR_NDIM
  - AARR_DIMS
  - AARR_LBOUND
  - ArrayGetNItems
  - [get_type_io_data](../g/get_type_io_data.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - array_iter_setup
  - array_iter_next
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [scanner_isspace](../s/scanner_isspace.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [anyarray_out](anyarray_out.md)
  - [anycompatiblearray_out](anycompatiblearray_out.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function uses an ArrayMetaState cache structure to store element type information, avoiding repeated type lookups for better performance. It handles special cases like empty strings and literal "NULL" values by forcing quotes. The function calculates exact memory requirements before constructing the output string to avoid buffer overflows. Multi-dimensional arrays include explicit dimension bounds (e.g., [1:3][1:2]) when lower bounds differ from 1. Character escaping follows PostgreSQL standards, with backslashes and double quotes being escaped with backslashes.