# multirange_minus

## Location
src/backend/utils/adt/multirangetypes.c: 1114 - 1143

## Overview
Computes the difference between two multirange values, removing all ranges from the second multirange from the first multirange.

## Definition
```c
Datum multirange_minus(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the minus (difference) operation for multirange types. It takes two multirange inputs and produces a new multirange containing all ranges from the first input that don't overlap with any ranges in the second input. The function includes optimizations for empty inputs: if either input is empty, it returns the first input directly. For non-empty inputs, it deserializes both multiranges and delegates the actual subtraction logic to the multirange_minus_internal function.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing two multirange arguments
  - Argument 0: First multirange operand (minuend)
  - Argument 1: Second multirange operand (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeTypeGetOid
  - multirange_get_typcache
  - MultirangeIsEmpty
  - PG_RETURN_MULTIRANGE_P
  - multirange_deserialize
  - multirange_minus_internal
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Implements the MINUS operator for multirange types
- Optimizes for empty operands: if either input is empty, returns the first input unchanged
- Delegates the complex subtraction logic to multirange_minus_internal function
- The actual range subtraction and fragmentation logic is handled by the internal function
- Located in src/backend/utils/adt/multirangetypes.c:1114-1143