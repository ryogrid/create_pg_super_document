# numeric_out

## Location
src/backend/utils/adt/numeric.c: 814 - 848

## Overview
The output function for PostgreSQL's numeric data type, responsible for converting internal Numeric values into their string representations.

## Definition
```c
Datum numeric_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_out` function serves as the standard output conversion function for PostgreSQL's arbitrary precision numeric data type. It handles the conversion of internal Numeric values to their external string representation, supporting:

1. **Special values**: Properly formats NaN, Infinity, and -Infinity as standardized strings
2. **Regular numeric values**: Converts finite numbers to their decimal string representation
3. **Precision preservation**: Maintains the exact precision and scale of the original numeric value

The function follows PostgreSQL's standard output function conventions and ensures that the string representation can be successfully parsed back by `numeric_in()`, maintaining round-trip conversion fidelity.

## Parameters / Member Variables
- `num`: Input Numeric value to be converted to string (PG_GETARG_NUMERIC(0))

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_SPECIAL`: Checks if value is NaN or infinity
  - `NUMERIC_IS_PINF`: Tests for positive infinity
  - `NUMERIC_IS_NINF`: Tests for negative infinity  
  - `[init_var_from_num](../i/init_var_from_num.md)`: Converts Numeric to NumericVar format
  - `[get_str_from_var](../g/get_str_from_var.md)`: Generates string from NumericVar
  - `[pstrdup](../p/pstrdup.md)`: Duplicates string in appropriate memory context
- Called from (representative examples):
  - `[numeric_to_cstring](numeric_to_cstring.md)`: Database size formatting
  - `[numeric_to_char](numeric_to_char.md)`: Text formatting operations
  - `[jsonb_put_escaped_value](../j/jsonb_put_escaped_value.md)`: JSON output formatting
  - `[JsonbValueAsText](../J/JsonbValueAsText.md)`: JSON value extraction
  - `[numeric_float8](numeric_float8.md)`/`numeric_float4`: Float conversion functions

## Notes and Other Information
- Returns palloced strings that are automatically freed by PostgreSQL's memory management
- Handles special values (NaN, ±Infinity) with standardized string representations matching SQL standards
- Ensures round-trip conversion compatibility with `numeric_in()`
- The output format is deterministic and platform-independent
- Used extensively in JSON operations, formatting functions, and type conversion scenarios