# ClearPgTm

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:94-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L94-L111)

## Overview
ClearPgTm initializes a time structure and fractional seconds field to zero values, providing a clean slate for time/date operations in ECPG.

## Definition

```c
static inline void
ClearPgTm(struct /* pg_ */ tm *tm, fsec_t *fsec)
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) pgtypeslib and serves as an initialization utility for time structures. It systematically zeros out all fields of a standard C library tm structure along with an associated fractional seconds field. The function is copied and adapted from the PostgreSQL backend datetime utilities but modified to work with the standard C library's struct tm instead of PostgreSQL's internal struct pg_tm.

The function performs a complete initialization by setting:
- All date components (year, month, day) to zero
- All time components (hour, minute, second) to zero  
- The fractional seconds field to zero

This provides a consistent starting point for subsequent time/date calculations and parsing operations.

## Parameters / Member Variables
- `*tm`: Pointer to a standard C library tm structure to be initialized
- `*fsec`: Pointer to fractional seconds field (fsec_t) to be zeroed
## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (type definition for fractional seconds)
- Called from (representative examples):
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md)
  - [DecodeInterval](../D/DecodeInterval.md)

## Notes and Other Information
- This is a static inline function within src/interfaces/ecpg/pgtypeslib/interval.c
- The function is specifically designed for ECPG's pgtypeslib and works with standard struct tm
- It's a copy-paste adaptation from the backend's datetime.c, modified for ECPG compatibility
- The backend equivalent for PostgreSQL's internal structures is ClearPgItmIn which operates on struct pg_itm_in
- All tm structure fields are set to 0, which may not represent valid dates but provides a clean initialization state
- Used primarily in interval parsing and decoding operations within ECPG
- The inline declaration suggests it's intended for performance-critical initialization operations