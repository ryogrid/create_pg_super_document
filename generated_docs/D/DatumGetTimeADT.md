# DatumGetTimeADT

## Location
[src/include/utils/date.h:60-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/date.h#L60-L65)

## Overview
DatumGetTimeADT is a static inline function that extracts a TimeADT value from a PostgreSQL Datum, providing a type-safe conversion mechanism for time values in PostgreSQL's function manager interface.

## Definition

```c
static inline TimeADT
DatumGetTimeADT(Datum X)
```
## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) system, specifically designed to extract time values from Datum objects. It internally delegates to DatumGetInt64() since TimeADT is fundamentally represented as a 64-bit integer in PostgreSQL, storing microseconds since midnight. The function provides type safety and clarity when working with time values in the PostgreSQL backend, ensuring that Datum values are properly interpreted as TimeADT types.

The function is defined as a static inline function in the header file, meaning it will be inlined at compile time for performance optimization. This is particularly important for time operations as they are frequently performed throughout PostgreSQL's date/time processing systems.

## Parameters / Member Variables
- : A PostgreSQL Datum containing a time value that needs to be converted to TimeADT format

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](DatumGetInt64.md) (underlying conversion mechanism)
  - TimeADT (target type for conversion)
- Called from (representative examples):
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md) (JSON encoding of time values)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md) (JSON path execution for time operations)
  - [convert_timevalue_to_scalar](../c/convert_timevalue_to_scalar.md) (statistics estimation for time values)
  - PG_GETARG_TIMEADT (function argument extraction macro)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion infrastructure for the function manager system
- The implementation leverages the fact that TimeADT is internally represented as a 64-bit integer containing microseconds since midnight
- As a static inline function, it provides zero-overhead abstraction for type conversion
- TimeADT uses pass-by-reference semantics if and only if int64 is passed by reference on the target platform
- The function is used throughout PostgreSQL's time processing, JSON operations, and statistical analysis
- Located in src/include/utils/date.h, making it available to all components that include this header file
- Works in conjunction with TimeADTGetDatum for bidirectional conversion between Datum and TimeADT types