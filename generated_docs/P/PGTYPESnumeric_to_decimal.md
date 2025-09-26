# PGTYPESnumeric_to_decimal

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1547-1569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1547-L1569)

## Overview
Converts a PostgreSQL numeric value to a decimal structure by copying all numeric fields with overflow checking.

## Definition
```c
int PGTYPESnumeric_to_decimal(numeric *src, decimal *dst)
```

## Detailed Description
This function performs a direct field-by-field copy from a PostgreSQL numeric structure to a decimal structure. It first checks that the source numeric has no more digits than the decimal structure can accommodate (DECSIZE limit), then copies all the numeric properties including weight, scale values, sign, digit count, and the actual digit array.

The function provides a bridge between PostgreSQL's internal numeric representation and the ECPG decimal type used in embedded SQL applications, particularly for Informix compatibility.

## Parameters / Member Variables
- `src`: Source numeric value to convert (pointer to numeric structure)
- `dst`: Destination decimal structure to populate (pointer to decimal structure)

## Dependencies
- Functions called/Symbols referenced:
  - [numeric](../n/numeric.md) (type)
  - decimal (type)
  - DECSIZE (maximum decimal size constant)
  - PGTYPES_NUM_OVERFLOW (error constant)
- Called from (representative examples):
  - [deccall3](../d/deccall3.md), deccvasc, deccvdbl, deccvint, deccvlong (in compatlib/informix.c)
  - [ecpg_get_data](../e/ecpg_get_data.md) (in ecpglib/data.c)
  - [main](../m/main.md) (in test files for numeric operations)

## Notes and Other Information
- Returns 0 on successful conversion, -1 on overflow error
- Performs overflow checking based on DECSIZE limit for decimal structures
- Direct field copying preserves all numeric precision and scale information
- Part of the ECPG compatibility layer for embedded SQL applications
- Primarily used for Informix compatibility in the ECPG system
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:1547-1569