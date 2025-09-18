# UInt64GetDatum

## Location
[src/include/postgres.h:436-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L436-L457)

## Overview
Converts a 64-bit unsigned integer to a PostgreSQL Datum representation, handling platform-specific differences in how 64-bit values are passed.

## Definition


## Detailed Description
UInt64GetDatum is a utility function that converts a 64-bit unsigned integer (uint64) into PostgreSQL's universal Datum type. The implementation varies based on compilation flags:

- When  is defined (typically on 64-bit platforms), the function directly casts the uint64 value to a Datum.
- When  is not defined (typically on 32-bit platforms), it delegates to  after casting the unsigned value to a signed int64.

This conditional compilation ensures optimal performance on 64-bit platforms while maintaining compatibility with 32-bit systems where 64-bit values must be passed by reference through allocated memory.

## Parameters / Member Variables
- : The 64-bit unsigned integer value to be converted to a Datum

## Dependencies
- Functions called/Symbols referenced:
  - [Int64GetDatum](../I/Int64GetDatum.md) (conditionally, when USE_FLOAT8_BYVAL is not defined)
  - USE_FLOAT8_BYVAL (compilation flag)
- Called from (representative examples):
  - [compute_partition_hash_value](../c/compute_partition_hash_value.md)
  - [hash_aclitem_extended](../h/hash_aclitem_extended.md)
  - [JsonbHashScalarValueExtended](../J/JsonbHashScalarValueExtended.md)
  - [hash_numeric_extended](../h/hash_numeric_extended.md)
  - PG_STAT_GET_ACTIVITY_COLS
  - [hash_any_extended](../h/hash_any_extended.md)
  - [hash_uint32_extended](../h/hash_uint32_extended.md)
  - PG_RETURN_UINT64
  - [FullTransactionIdGetDatum](../F/FullTransactionIdGetDatum.md)

## Notes and Other Information
- The function is declared as  for performance optimization
- When int64 is passed by reference (32-bit platforms), this function returns a reference to palloc'd space
- Part of PostgreSQL's type conversion system that ensures consistent handling of data types across different architectures
- Located in src/include/postgres.h:436-457