# interval_hash_extended

## Location
src/backend/utils/adt/timestamp.c: 2611 - 2630

## Overview
The interval_hash_extended function computes an extended hash value for PostgreSQL Interval data types using an additional seed value, providing improved hash distribution for advanced hashing scenarios.

## Definition
```c
Datum interval_hash_extended(PG_FUNCTION_ARGS)
```

## Detailed Description
This function generates an extended hash value for an Interval by converting the interval to a standardized span representation using `interval_cmp_value`, then computing a hash using both the span value and an additional seed parameter. Like `interval_hash`, it uses only the least significant 64 bits of the 128-bit span for compatibility reasons. The extended hash function is used in scenarios where better hash distribution is needed, such as in hash partitioning or when combining multiple hash values. The function ensures that equal intervals with the same seed produce identical hash values.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: Interval pointer to be hashed
  - Argument 1: Seed value (Datum) for extended hashing

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P`: Extracts Interval argument from function call context
  - `PG_GETARG_DATUM`: Extracts seed argument from function call context
  - `interval_cmp_value`: Converts interval to standardized 128-bit span representation
  - `int128_to_int64`: Converts 128-bit span to 64-bit for hashing compatibility
  - `hashint8extended`: Computes extended hash value for the 64-bit span and seed
  - `DirectFunctionCall2`: Calls hashint8extended with span and seed values
  - `Int64GetDatumFast`: Converts int64 to PostgreSQL Datum format
  - `Interval`: PostgreSQL interval data type structure
  - `INT128`: 128-bit integer type used for intermediate calculations

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:2611-2630
- Uses the same span calculation approach as interval_hash for consistency
- The additional seed parameter enables better hash distribution in certain scenarios
- Only uses the least significant 64 bits for backward compatibility
- Used in advanced hashing scenarios like hash partitioning where improved distribution is beneficial
- Maintains the critical property that equal intervals with the same seed produce equal hash values