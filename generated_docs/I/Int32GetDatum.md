# Int32GetDatum

## Location
src/include/postgres.h: 212 - 221

## Overview
Converts a 32-bit signed integer value into PostgreSQL's internal Datum representation for use throughout the database system.

## Definition


## Detailed Description
Int32GetDatum is a static inline function that provides type-safe conversion from a 32-bit signed integer (int32) to PostgreSQL's universal Datum type. This function is part of PostgreSQL's datum conversion system that enables uniform handling of different data types within the database engine. The conversion is implemented as a direct cast, taking advantage of the fact that 32-bit signed integers can be stored directly in the Datum representation without additional encoding or memory allocation.

## Parameters / Member Variables
- : The 32-bit signed integer value to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple cast operation)
- Called from (representative examples):
  - (No direct references found in the current codebase analysis)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a direct cast from int32 to Datum, which is efficient and requires no runtime overhead
- Companion function to DatumGetInt32, forming a bidirectional conversion pair
- Part of PostgreSQL's type system infrastructure that enables uniform handling of different data types
- Though no direct references were found in this analysis, this function is likely used extensively through macro expansions and in code paths that handle 32-bit integer values
- Critical for functions that need to return int32 values as Datum results in PostgreSQL's function call interface