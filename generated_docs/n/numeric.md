# numeric

## Location
[src/backend/utils/adt/numeric.c:1244-1321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1244-L1321)

## Overview
The  function is a special PostgreSQL type coercion function that applies precision and scale constraints to numeric values before they are stored in tuple attributes.

## Definition

```c
Datum
numeric		(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a type modifier enforcer for the NUMERIC data type in PostgreSQL. When a value is about to be stored in a column with specific precision and scale constraints, this function is called to ensure the value conforms to those constraints. The function handles special cases like NaN and infinity values, performs bounds checking, and applies rounding when necessary.

The function implements an optimization where if the input value already fits within the target constraints without requiring rounding, it simply creates a copy with adjusted scale fields rather than performing expensive arithmetic operations.

## Parameters / Member Variables
- : The input NUMERIC value to be type-modified (PG_GETARG_NUMERIC(0))
- : The type modifier containing precision and scale information (PG_GETARG_INT32(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extracts numeric argument from function call
  - PG_GETARG_INT32: Extracts integer argument from function call
  - NUMERIC_IS_SPECIAL: Checks if numeric is NaN or infinity
  - [apply_typmod_special](../a/apply_typmod_special.md): Handles special values (NaN/infinity)
  - [duplicate_numeric](../d/duplicate_numeric.md): Creates a copy of numeric value
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md): Validates type modifier
  - [numeric_typmod_precision](numeric_typmod_precision.md): Extracts precision from typmod
  - [numeric_typmod_scale](numeric_typmod_scale.md): Extracts scale from typmod
  - NUMERIC_CAN_BE_SHORT: Checks if value can use short representation
  - init_var: Initializes NumericVar structure
  - [set_var_from_num](../s/set_var_from_num.md): Converts Numeric to NumericVar
  - [apply_typmod](../a/apply_typmod.md): Applies type modifier constraints
  - [make_result](../m/make_result.md): Converts NumericVar back to Numeric
  - [free_var](../f/free_var.md): Frees NumericVar memory
  - PG_RETURN_NUMERIC: Returns numeric result

- Called from (representative examples):
  - Type coercion operations throughout the system
  - JSONB conversion functions (jsonb_numeric, jsonb_int2, etc.)
  - ECPG interface functions
  - Various numeric conversion and manipulation functions

## Notes and Other Information
- This function is automatically invoked by PostgreSQL's type system when type coercion is needed
- Implements performance optimizations for cases where no actual computation is required
- Handles both short and long numeric representations efficiently
- Critical for maintaining data integrity in numeric columns with precision/scale constraints
- Part of the core numeric type implementation in src/backend/utils/adt/numeric.c:1244-1321