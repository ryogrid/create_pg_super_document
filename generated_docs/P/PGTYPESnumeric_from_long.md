# PGTYPESnumeric_from_long

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1318-1387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1318-L1387)

## Overview
Converts a signed long integer value into PostgreSQL's numeric type representation, handling sign conversion and decimal formatting.

## Definition
```c
int PGTYPESnumeric_from_long(signed long int long_val, numeric *var)
```

## Detailed Description
This function converts a signed long integer into PostgreSQL's internal numeric representation. It calculates the number of digits needed to represent the integer, allocates appropriate memory for the numeric variable, and stores the digits in the numeric format with a decimal scale of 1 (effectively adding a ".0" to the integer). The function handles both positive and negative values by setting the appropriate sign and working with the absolute value during conversion.

The conversion process involves:
1. Determining the sign and calculating the absolute value
2. Computing the number of digits required through iterative multiplication by 10
3. Allocating memory for the numeric variable
4. Converting digits by extracting them using division and modulo operations
5. Setting appropriate scale and weight values for the numeric representation

## Parameters / Member Variables
- `long_val`: The signed long integer value to convert to numeric format
- `var`: Pointer to the numeric structure that will store the converted value

## Dependencies
- Functions called/Symbols referenced:
  - [alloc_var](../a/alloc_var.md) (allocates memory for numeric variable)
  - NUMERIC_NEG (constant for negative sign)
  - NUMERIC_POS (constant for positive sign)
  - [numeric](../n/numeric.md) (type definition)
- Called from (representative examples):
  - [deccvlong](../d/deccvlong.md) (in informix compatibility library)
  - [PGTYPESnumeric_from_int](PGTYPESnumeric_from_int.md) (wrapper function)
  - [main](../m/main.md) (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (typically memory allocation failure)
- The resulting numeric has rscale=1 and dscale=1, effectively representing the integer with one decimal place
- Handles LONG_MAX boundary conditions to prevent overflow during digit calculation
- Memory is properly initialized, allowing early termination when abs_long_val reaches 0
- Part of the ECPG (Embedded C for PostgreSQL) pgtypes library for type conversions