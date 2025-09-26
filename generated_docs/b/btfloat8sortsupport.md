# btfloat8sortsupport

## Location
[src/backend/utils/adt/float.c:985-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L985-L993)

## Overview
PostgreSQL function that configures sort support for double-precision floating-point B-tree operations by setting up an optimized comparison function.

## Definition

```c
Datum
btfloat8sortsupport(PG_FUNCTION_ARGS)
```
## Detailed Description
This function initializes a SortSupport structure for double-precision floating-point values used in B-tree indexing operations. It assigns the optimized btfloat8fastcmp function as the comparator, which enables faster sorting by bypassing the standard PostgreSQL function call overhead. This is part of PostgreSQL's sort support infrastructure that allows data types to provide specialized, high-performance comparison routines for sorting and indexing operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Pointer to a SortSupport structure that needs to be configured

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro for extracting pointer arguments)
  - [SortSupport](../S/SortSupport.md) (type definition for sort support structure)
  - [btfloat8fastcmp](btfloat8fastcmp.md) (assigned as the comparator function)
  - PG_RETURN_VOID (macro for returning void)

- Called from (representative examples):
  - No direct references found in the codebase (likely referenced through system catalogs for float8 B-tree operations)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:985-993
- This function is called by the PostgreSQL system when setting up sort support for float8 columns in B-tree indexes
- By assigning btfloat8fastcmp as the comparator, it enables optimized sorting performance for double-precision values
- The SortSupport framework allows PostgreSQL to use faster comparison functions that avoid function call overhead
- Essential for efficient sorting and indexing operations on double-precision floating-point columns
- Returns void since it only configures the SortSupport structure