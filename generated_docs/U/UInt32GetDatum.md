# UInt32GetDatum

## Location
src/include/postgres.h: 232 - 241

## Overview
Converts a 32-bit unsigned integer value into PostgreSQL's internal Datum representation for use throughout the database system.

## Definition


## Detailed Description
UInt32GetDatum is a static inline function that provides type-safe conversion from a 32-bit unsigned integer (uint32) to PostgreSQL's universal Datum type. This function is part of PostgreSQL's datum conversion system that enables uniform handling of different data types within the database engine. The conversion is implemented as a direct cast, taking advantage of the fact that 32-bit unsigned integers can be stored directly in the Datum representation without additional encoding or memory allocation.

## Parameters / Member Variables
- : The 32-bit unsigned integer value to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple cast operation)
- Called from (representative examples):
  - [directBoolConsistentFn](../d/directBoolConsistentFn.md) (src/backend/access/gin/ginlogic.c:78)
  - _hash_convert_tuple (src/backend/access/hash/hashutil.c:332)
  - [LaunchParallelWorkers](../L/LaunchParallelWorkers.md) (src/backend/access/transam/parallel.c:601)
  - [jsonb_path_ops__extract_nodes](../j/jsonb_path_ops__extract_nodes.md) (src/backend/utils/adt/jsonb_gin.c:489)
  - [pg_lock_status](../p/pg_lock_status.md) (src/backend/utils/adt/lockfuncs.c:273, 284)
  - PG_RETURN_UINT32 macro (src/include/fmgr.h:355)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it available throughout the PostgreSQL codebase
- The function performs a direct cast from uint32 to Datum, which is efficient and requires no runtime overhead
- Companion function to DatumGetUInt32, forming a bidirectional conversion pair
- Particularly important for hash functions, GIN index operations, and parallel worker management
- Used extensively in contexts requiring unsigned integer return values, such as hash computations and lock status reporting
- Critical for functions that need to return uint32 values as Datum results in PostgreSQL's function call interface
- Part of PostgreSQL's type system infrastructure that enables uniform handling of different data types