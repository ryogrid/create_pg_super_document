# PGTYPESnumeric_from_decimal

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1570-1588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1570-L1588)

## Overview
Converts a decimal structure to a PostgreSQL numeric value by initializing the numeric and copying all decimal fields with proper memory allocation.

## Definition
```c
int PGTYPESnumeric_from_decimal(decimal *src, numeric *dst)
```

## Detailed Description
This function converts a decimal structure to a PostgreSQL numeric structure by first initializing the destination numeric to zero, then copying all the decimal properties and allocating appropriate memory for the digit array. The function ensures proper memory management by using `zero_var` to initialize and `alloc_var` to allocate space for the required number of digits.

This function serves as the reverse operation of `PGTYPESnumeric_to_decimal`, providing conversion from ECPG decimal types back to PostgreSQL's internal numeric representation.

## Parameters / Member Variables
- `src`: Source decimal structure to convert (pointer to decimal structure)
- `dst`: Destination numeric structure to populate (pointer to numeric structure)

## Dependencies
- Functions called/Symbols referenced:
  - [numeric](../n/numeric.md) (type)
  - decimal (type)
  - [zero_var](../z/zero_var.md) (numeric initialization function)
  - [alloc_var](../a/alloc_var.md) (numeric memory allocation function)
- Called from (representative examples):
  - [deccall2](../d/deccall2.md), deccall3 (in compatlib/informix.c)
  - [dectoasc](../d/dectoasc.md), dectodbl, dectoint, dectolong (in compatlib/informix.c)
  - [ecpg_store_input](../e/ecpg_store_input.md) (in ecpglib/execute.c)
  - [main](../m/main.md) (in test files for numeric operations)

## Notes and Other Information
- Returns 0 on successful conversion, -1 on memory allocation failure
- Properly initializes the destination numeric structure before copying
- Uses PostgreSQL's internal memory allocation functions for numeric digit arrays
- Part of the ECPG compatibility layer for embedded SQL applications
- Essential for Informix compatibility in the ECPG system
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:1570-1588