# TimeADTGetDatum

## Location
[src/include/utils/date.h:78-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/date.h#L78-L83)

## Overview
TimeADTGetDatum is a static inline function that converts a TimeADT value to a PostgreSQL Datum, providing a type-safe conversion mechanism for returning time values in PostgreSQL's function manager interface.

## Definition

```c
struct pg_tm *tm, fsec_t *fsec);
```
## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) system, specifically designed to convert TimeADT values into Datum objects for return values or storage. It internally delegates to Int64GetDatum() since TimeADT is fundamentally represented as a 64-bit integer containing microseconds since midnight in PostgreSQL. The function provides type safety and clarity when working with time values in the PostgreSQL backend, ensuring that TimeADT values are properly packaged as Datum types.

The function is defined as a static inline function in the header file, meaning it will be inlined at compile time for performance optimization. This is the complementary function to DatumGetTimeADT, providing bidirectional conversion between Datum and TimeADT types.

## Parameters / Member Variables
- : A TimeADT value that needs to be converted to Datum format for return or storage

## Dependencies
- Functions called/Symbols referenced:
  - [Int64GetDatum](../I/Int64GetDatum.md) (underlying conversion mechanism)
  - TimeADT (source type for conversion)
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md) (SQL function execution)
  - [parse_datetime](../p/parse_datetime.md) (date/time parsing operations)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md) (JSON path execution for time operations)
  - PG_RETURN_TIMEADT (function return value macro)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion infrastructure for the function manager system
- The implementation leverages the fact that TimeADT is internally represented as a 64-bit integer storing microseconds since midnight
- As a static inline function, it provides zero-overhead abstraction for type conversion
- This is the inverse operation of DatumGetTimeADT, enabling bidirectional conversion between Datum and TimeADT
- The function is used throughout PostgreSQL's time processing, JSON path operations, and SQL function execution
- Located in src/include/utils/date.h, making it available to all components that include this header file
- Essential for returning time values from PostgreSQL functions that use the fmgr interface
- TimeADT uses pass-by-reference semantics if and only if int64 is passed by reference on the target platform
- Works seamlessly with PostgreSQL's internal time representation and precision requirements