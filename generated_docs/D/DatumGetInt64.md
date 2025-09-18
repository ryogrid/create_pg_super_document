# DatumGetInt64

## Location
[src/include/postgres.h:385-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L385-L402)

## Overview
DatumGetInt64 extracts a 64-bit signed integer value from a Datum, handling both pass-by-value and pass-by-reference storage methods transparently based on platform capabilities.

## Definition


## Detailed Description
DatumGetInt64 is a platform-aware function that extracts a 64-bit signed integer from a Datum representation. The implementation varies based on whether the platform supports pass-by-value for 64-bit integers (controlled by the USE_FLOAT8_BYVAL compilation flag).

On platforms where 64-bit integers can be passed by value (USE_FLOAT8_BYVAL is defined), the function directly casts the Datum to int64. On platforms where 64-bit values must be passed by reference, it dereferences the pointer obtained via DatumGetPointer().

This abstraction allows the same code to work correctly across different platforms with varying capabilities for handling 64-bit values, making PostgreSQL portable across 32-bit and 64-bit architectures.

## Parameters / Member Variables
- : A Datum containing a 64-bit signed integer value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md) (on pass-by-reference platforms)
  - USE_FLOAT8_BYVAL (preprocessor flag)
- Called from (representative examples):
  - [btint8fastcmp](../b/btint8fastcmp.md) (B-tree comparison for int8)
  - [defGetInt64](../d/defGetInt64.md) (option parsing)
  - [recompute_limits](../r/recompute_limits.md) (LIMIT/OFFSET processing)
  - [generate_series_int8_support](../g/generate_series_int8_support.md) (series generation)
  - PG_GETARG_INT64 (function manager macro)
  - DatumGetTimestamp (timestamp handling)

## Notes and Other Information
- This function abstracts the platform-specific differences in 64-bit integer handling
- On 64-bit platforms with USE_FLOAT8_BYVAL, int64 values are passed directly in the Datum
- On 32-bit platforms without USE_FLOAT8_BYVAL, int64 values are passed by reference
- The USE_FLOAT8_BYVAL flag is typically defined on 64-bit platforms where pointers are 64 bits
- This design ensures optimal performance on each platform while maintaining code portability
- Used extensively for timestamp, interval, and large integer operations in PostgreSQL