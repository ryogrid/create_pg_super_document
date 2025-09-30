# DateADTGetDatum

## Location
[src/include/utils/date.h:72-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/date.h#L72-L77)

## Overview
DateADTGetDatum is a static inline function that converts a DateADT value to a PostgreSQL Datum, providing a type-safe conversion mechanism for returning date values in PostgreSQL's function manager interface.

## Definition

```c
static inline Datum
DateADTGetDatum(DateADT X)
```
## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) system, specifically designed to convert DateADT values into Datum objects for return values or storage. It internally delegates to Int32GetDatum() since DateADT is fundamentally represented as a 32-bit integer in PostgreSQL. The function provides type safety and clarity when working with date values in the PostgreSQL backend, ensuring that DateADT values are properly packaged as Datum types.

The function is defined as a static inline function in the header file, meaning it will be inlined at compile time for performance optimization. This is the complementary function to DatumGetDateADT, providing bidirectional conversion between Datum and DateADT types.

## Parameters / Member Variables
- : A DateADT value that needs to be converted to Datum format for return or storage

## Dependencies
- Functions called/Symbols referenced:
  - [Int32GetDatum](../I/Int32GetDatum.md) (underlying conversion mechanism)
  - DateADT (source type for conversion)
- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md) (SQL function execution)
  - [parse_datetime](../p/parse_datetime.md) (date/time parsing operations)
  - [daterange_canonical](../d/daterange_canonical.md) (range type canonicalization)
  - PG_RETURN_DATEADT (function return value macro)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion infrastructure for the function manager system
- The implementation leverages the fact that DateADT is internally represented as a 32-bit integer
- As a static inline function, it provides zero-overhead abstraction for type conversion
- This is the inverse operation of DatumGetDateADT, enabling bidirectional conversion between Datum and DateADT
- The function is used throughout PostgreSQL's date processing, range operations, and SQL function execution
- Located in src/include/utils/date.h, making it available to all components that include this header file
- Essential for returning date values from PostgreSQL functions that use the fmgr interface
- Works seamlessly with PostgreSQL's pass-by-value semantics for 32-bit integer types

## Simplified Source

```c
static inline Datum DateADTGetDatum(DateADT X) {
    // Convert DateADT (which is internally a 32-bit integer) to Datum
    return Int32GetDatum(X);
}
```