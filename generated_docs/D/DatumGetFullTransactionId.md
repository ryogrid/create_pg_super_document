# DatumGetFullTransactionId

## Location
[src/include/utils/xid8.h:18-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/xid8.h#L18-L23)

## Overview
Converts a PostgreSQL Datum value to a FullTransactionId, enabling the conversion of 8-byte transaction IDs from SQL data types to internal PostgreSQL transaction ID structures.

## Definition
```c
static inline FullTransactionId DatumGetFullTransactionId(Datum X)
```

## Detailed Description
This inline function serves as a type conversion utility in PostgreSQL's xid8 Abstract Data Type (ADT) system. It takes a Datum (PostgreSQL's generic data type for SQL values) and converts it to a FullTransactionId structure. The function acts as a bridge between PostgreSQL's SQL layer and its internal transaction management system, specifically handling 64-bit transaction IDs that include both epoch and transaction ID components.

The conversion is performed by first extracting the underlying 64-bit unsigned integer from the Datum using `DatumGetUInt64`, then constructing a FullTransactionId from that value using `FullTransactionIdFromU64`. This ensures proper type safety and prevents implicit conversions between different transaction ID representations.

## Parameters / Member Variables
- `X`: A Datum containing a 64-bit transaction ID value that needs to be converted to a FullTransactionId structure

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionIdFromU64](../F/FullTransactionIdFromU64.md)
  - [DatumGetUInt64](DatumGetUInt64.md)
- Called from (representative examples):
  - PG_GETARG_FULLTRANSACTIONID

## Notes and Other Information
- This is a static inline function defined in src/include/utils/xid8.h:18-23
- Part of PostgreSQL's xid8 ADT which handles 64-bit transaction IDs
- Used primarily in SQL function implementations that need to work with extended transaction IDs
- The function ensures type safety by preventing direct casting between Datum and FullTransactionId
- Commonly used through the PG_GETARG_FULLTRANSACTIONID macro for extracting function arguments

## Simplified Source

```c
static inline FullTransactionId
DatumGetFullTransactionId(Datum X)
{
    // Convert Datum to 64-bit integer, then to FullTransactionId
    return FullTransactionIdFromU64(DatumGetUInt64(X));
}
```