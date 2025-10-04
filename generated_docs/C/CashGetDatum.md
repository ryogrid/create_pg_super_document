# CashGetDatum

## Location
[src/include/utils/cash.h:27-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/cash.h#L27-L31)

## Overview
CashGetDatum is an inline utility function that converts a Cash value to a Datum value, enabling Cash values to be stored in PostgreSQL's generic Datum container type.

## Definition
```c
static inline Datum CashGetDatum(Cash X)
```

## Detailed Description
CashGetDatum performs the conversion from Cash type to Datum type by delegating to Int64GetDatum(). Since Cash is typedef'd as int64, this conversion is straightforward and leverages the existing int64-to-Datum conversion infrastructure. The function is marked as static inline for optimal performance, as it's a simple wrapper that should be inlined at compile time. This function is essential for the PostgreSQL type system, allowing Cash values to be passed through the generic Datum interface used throughout the database engine.

## Parameters / Member Variables
- `X`: The Cash value to be converted to Datum type

## Dependencies
- Functions called/Symbols referenced:
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - Cash (type)
- Called from (representative examples):
  - PG_RETURN_CASH

## Notes and Other Information
- Cash is typedef'd as int64, ensuring direct compatibility with int64 conversion functions
- The function is the inverse operation of DatumGetCash
- Pass-by-reference behavior follows int64 conventions
- This is a header-only inline function defined in src/include/utils/cash.h
- Critical for PostgreSQL's monetary data type system integration
- Enables Cash values to participate in the standard PostgreSQL function calling conventions

## Simplified Source

```c
static inline Datum CashGetDatum(Cash X) {
    // Convert Cash (which is int64) to Datum
    return Int64GetDatum(X);
}
```