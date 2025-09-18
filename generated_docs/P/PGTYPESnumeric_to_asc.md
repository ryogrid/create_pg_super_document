# PGTYPESnumeric_to_asc

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 343 - 373

## Overview
Converts a PostgreSQL numeric type structure to its string representation with specified decimal scale for use in ECPG applications.

## Definition
```c
char *PGTYPESnumeric_to_asc(numeric *num, int dscale)
```

## Detailed Description
This function converts a numeric value to its string representation. It creates a working copy of the input numeric value to avoid modifying the original, then uses the internal `get_str_from_var` function to generate the string representation with the specified decimal scale. The function handles memory allocation and ensures proper cleanup of temporary resources.

## Parameters / Member Variables
- `num`: Pointer to the numeric structure to be converted to string
- `dscale`: Number of decimal places to include in the output string; if negative, uses the original scale from the numeric value

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_new](PGTYPESnumeric_new.md) (for creating working copy)
  - [PGTYPESnumeric_copy](PGTYPESnumeric_copy.md) (for copying numeric value)
  - [PGTYPESnumeric_free](PGTYPESnumeric_free.md) (for cleanup)
  - [get_str_from_var](../g/get_str_from_var.md) (for string conversion)
  - [numeric](../n/numeric.md) (type definition)
- Called from (representative examples):
  - [dectoasc](../d/dectoasc.md) (Informix compatibility function)
  - ecpg_store_input (ECPG input processing)
  - PGTYPESnumeric_to_long (numeric conversion)
  - Various test functions

## Notes and Other Information
- Returns NULL on memory allocation failure or copy error
- Creates a temporary copy to avoid modifying the original numeric value
- The dscale parameter allows control over decimal precision in output
- When dscale is negative, preserves the original decimal scale
- Caller is responsible for freeing the returned string
- Part of the ECPG pgtypes library for PostgreSQL embedded SQL
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:343-373