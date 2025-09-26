# DatumGetLSN

## Location
[src/include/utils/pg_lsn.h:22-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pg_lsn.h#L22-L27)

## Overview
DatumGetLSN is an inline function that converts a PostgreSQL Datum value to an XLogRecPtr (Log Sequence Number), providing a type-safe way to extract LSN values from the PostgreSQL type system.

## Definition
static inline XLogRecPtr DatumGetLSN(Datum X)

## Detailed Description
DatumGetLSN is a simple conversion function that takes a Datum (PostgreSQL's generic data type) and converts it to an XLogRecPtr by first extracting the underlying 64-bit integer value using DatumGetInt64() and then casting it to XLogRecPtr. This function is essential for handling LSN values in PostgreSQL's type system, where LSNs are represented as 64-bit unsigned integers but need to be converted to the appropriate XLogRecPtr type for use in transaction log operations.

The function is implemented as a static inline function in the header file, making it very efficient as it gets inlined at compile time rather than requiring a function call.

## Parameters / Member Variables
- `X`: A Datum value containing an LSN encoded as a 64-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](DatumGetInt64.md)
- Called from (representative examples):
  - [GetSubscriptionRelState](../G/GetSubscriptionRelState.md)
  - [GetSubscriptionRelations](../G/GetSubscriptionRelations.md)
  - [parse_subscription_options](../p/parse_subscription_options.md)
  - [libpqrcv_create_slot](../l/libpqrcv_create_slot.md)
  - PG_GETARG_LSN (macro)

## Notes and Other Information
- This function is the counterpart to LSNGetDatum(), which performs the reverse conversion
- It's commonly used through the PG_GETARG_LSN(n) macro which combines PG_GETARG_DATUM(n) with DatumGetLSN()
- LSNs (Log Sequence Numbers) are fundamental to PostgreSQL's WAL (Write-Ahead Logging) system
- The function assumes the input Datum contains a valid 64-bit integer representation of an LSN
- Being an inline function, it has no runtime overhead when optimizations are enabled