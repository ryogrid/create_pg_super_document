# ClearPgTm

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 94 - 111

## Overview
ClearPgTm initializes a time structure and fractional seconds field to zero values, providing a clean slate for time/date operations in ECPG.

## Definition


## Detailed Description
This function is part of the ECPG (Embedded SQL in C for PostgreSQL) pgtypeslib and serves as an initialization utility for time structures. It systematically zeros out all fields of a standard C library tm structure along with an associated fractional seconds field. The function is copied and adapted from the PostgreSQL backend datetime utilities but modified to work with the standard C library's struct tm instead of PostgreSQL's internal struct pg_tm.

The function performs a complete initialization by setting:
- All date components (year, month, day) to zero
- All time components (hour, minute, second) to zero  
- The fractional seconds field to zero

This provides a consistent starting point for subsequent time/date calculations and parsing operations.

## Parameters / Member Variables
- : Pointer to a standard C library tm structure to be initialized
- : Pointer to fractional seconds field (fsec_t) to be zeroed

## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (type definition for fractional seconds)
- Called from (representative examples):
  - DecodeISO8601Interval
  - DecodeInterval

## Notes and Other Information
- This is a static inline function within src/interfaces/ecpg/pgtypeslib/interval.c
- The function is specifically designed for ECPG's pgtypeslib and works with standard struct tm
- It's a copy-paste adaptation from the backend's datetime.c, modified for ECPG compatibility
- The backend equivalent for PostgreSQL's internal structures is ClearPgItmIn which operates on struct pg_itm_in
- All tm structure fields are set to 0, which may not represent valid dates but provides a clean initialization state
- Used primarily in interval parsing and decoding operations within ECPG
- The inline declaration suggests it's intended for performance-critical initialization operations