# multirange_union

## Location
src/backend/utils/adt/multirangetypes.c: 1082 - 1113

## Overview
Computes the union of two multirange values, combining all ranges from both inputs into a single multirange result.

## Definition
```c
Datum multirange_union(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the union operation for multirange types. It takes two multirange inputs and produces a new multirange containing all ranges from both inputs. The function includes optimizations for empty inputs: if either input is empty, it returns the other input directly. For non-empty inputs, it deserializes both multiranges, concatenates their range arrays, and constructs a new multirange. The make_multirange function handles the actual merging and normalization of overlapping ranges.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing two multirange arguments
  - Argument 0: First multirange operand
  - Argument 1: Second multirange operand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - PG_RETURN_MULTIRANGE_P
  - multirange_get_typcache
  - MultirangeTypeGetOid
  - multirange_deserialize
  - make_multirange
  - palloc0
  - memcpy
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Implements the UNION operator for multirange types
- Optimizes for empty operands by returning the non-empty operand directly
- Handles range merging and normalization automatically through make_multirange
- Uses dynamic memory allocation to accommodate the combined range arrays
- The actual union logic (merging overlapping ranges) is handled by make_multirange
- Located in src/backend/utils/adt/multirangetypes.c:1082-1113