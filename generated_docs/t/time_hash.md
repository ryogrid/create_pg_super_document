# time_hash

## Location
[src/backend/utils/adt/date.c:1747-1752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1747-L1752)

## Overview
The time_hash function computes a hash value for PostgreSQL's TimeADT data type, enabling efficient hashing operations for time values in hash tables and indexes.

## Definition

```c
Datum
time_hash(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the hash operation for PostgreSQL's time data type (TimeADT). It delegates the actual hashing computation to the hashint8 function, treating the time value as a 64-bit integer. This approach leverages the fact that TimeADT is internally represented as a 64-bit integer containing microseconds since midnight, making it suitable for direct hashing using the int8 hash algorithm.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the time value to be hashed
## Dependencies
- Functions called/Symbols referenced:
  - [hashint8](../h/hashint8.md)
- Called from (representative examples):
  - Used internally by PostgreSQL's hash-based operations on time data types

## Notes and Other Information
- The function is a simple wrapper around hashint8, maintaining consistency with PostgreSQL's approach of reusing existing hash functions for similar data representations
- Located in src/backend/utils/adt/date.c at lines 1747-1752
- Part of PostgreSQL's type system infrastructure for supporting hash-based operations on time values

## Simplified Source

```c
Datum
time_hash(PG_FUNCTION_ARGS)
{
    // Delegate to int8 hash function since TimeADT is a 64-bit integer
    return hashint8(fcinfo);
}
```