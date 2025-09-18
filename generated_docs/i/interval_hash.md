# interval_hash

## Location
src/backend/utils/adt/timestamp.c: 2593 - 2610

## Overview
The interval_hash function computes a hash value for PostgreSQL Interval data types, ensuring that equal intervals produce identical hash values for use in hash tables and hash indexes.

## Definition
```c
Datum interval_hash(PG_FUNCTION_ARGS)
```

## Detailed Description
This function generates a hash value for an Interval by first converting the interval to a standardized span representation using `interval_cmp_value`, then hashing only the least significant 64 bits of that span. The function ensures that intervals considered equal by `interval_cmp_internal` produce identical hash values, which is critical for hash-based operations like hash joins and hash indexes. For compatibility with earlier PostgreSQL versions that used only 64-bit arithmetic, only the lower 64 bits of the 128-bit span are used for hashing.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Interval pointer to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P`: Extracts Interval argument from function call context
  - [interval_cmp_value](interval_cmp_value.md): Converts interval to standardized 128-bit span representation
  - [int128_to_int64](int128_to_int64.md): Converts 128-bit span to 64-bit for hashing compatibility
  - [hashint8](../h/hashint8.md): Computes hash value for the 64-bit span
  - `DirectFunctionCall1`: Calls hashint8 with the span value
  - `Int64GetDatumFast`: Converts int64 to PostgreSQL Datum format
  - `Interval`: PostgreSQL interval data type structure
  - `INT128`: 128-bit integer type used for intermediate calculations

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2593-2610
- Only uses the least significant 64 bits for backward compatibility with pre-INT128 implementations
- Critical that equal intervals (per interval_cmp_internal) produce equal hash values
- Used by PostgreSQL for hash-based query operations like hash joins and hash indexes
- The upper 64 bits of the span are intentionally ignored as they seldom provide useful hash distribution