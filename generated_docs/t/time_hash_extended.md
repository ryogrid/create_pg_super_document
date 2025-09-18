# time_hash_extended

## Location
[src/backend/utils/adt/date.c:1753-1758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1753-L1758)

## Overview
The time_hash_extended function computes an extended hash value for PostgreSQL's TimeADT data type, providing enhanced hash distribution with an additional seed parameter for advanced hashing scenarios.

## Definition
```c
Datum time_hash_extended(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the extended hash operation for PostgreSQL's time data type (TimeADT). It delegates the computation to the hashint8extended function, which provides improved hash distribution by incorporating an additional 64-bit seed value. Like time_hash, it treats the time value as a 64-bit integer representing microseconds since midnight, but with enhanced collision resistance through the extended hashing algorithm.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the time value and seed for extended hashing

## Dependencies
- Functions called/Symbols referenced:
  - [hashint8extended](../h/hashint8extended.md)
- Called from (representative examples):
  - Used internally by PostgreSQL's hash-based operations requiring extended hash functions

## Notes and Other Information
- The function is a simple wrapper around hashint8extended, maintaining consistency with PostgreSQL's extended hashing infrastructure
- Located in src/backend/utils/adt/date.c at lines 1753-1758
- Provides better hash distribution than the basic time_hash function when used with different seed values