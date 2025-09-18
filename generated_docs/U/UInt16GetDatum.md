# UInt16GetDatum

## Location
src/include/postgres.h: 192 - 201

## Overview
Converts a 16-bit unsigned integer value into PostgreSQL's internal Datum representation for use in the database system.

## Definition


## Detailed Description
UInt16GetDatum is a static inline function that provides a type-safe conversion from a 16-bit unsigned integer (uint16) to PostgreSQL's universal Datum type. This function is part of PostgreSQL's datum conversion system, which allows different data types to be uniformly handled within the database engine. The conversion is implemented as a simple cast, taking advantage of the fact that uint16 values can be directly stored in the Datum representation without additional processing.

## Parameters / Member Variables
- : The 16-bit unsigned integer value to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple cast operation)
- Called from (representative examples):
  - GinFormTuple (src/backend/access/gin/ginentrypage.c:62)
  - collectMatchBitmap (src/backend/access/gin/ginget.c:197)
  - ProcedureCreate (src/backend/catalog/pg_proc.c:315-316)
  - pg_lock_status (src/backend/utils/adt/lockfuncs.c:285)
  - PG_RETURN_UINT16 macro (src/include/fmgr.h:357)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a direct cast from uint16 to Datum, which is efficient and requires no runtime overhead
- Commonly used in GIN index operations and various system functions that need to work with 16-bit unsigned integer values
- Part of PostgreSQL's type system infrastructure that enables uniform handling of different data types