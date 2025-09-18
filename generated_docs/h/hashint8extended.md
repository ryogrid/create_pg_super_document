# hashint8extended

## Location
[src/backend/access/hash/hashfunc.c:103-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L103-L115)

## Overview
The hashint8extended function computes an extended hash value for a 64-bit signed integer (int8) using an additional 64-bit seed value, while maintaining cross-type hash compatibility with smaller integer types.

## Definition
Datum hashint8extended(PG_FUNCTION_ARGS)

## Detailed Description
This function extends the hashint8 algorithm by incorporating an additional 64-bit seed value for enhanced hash distribution. It uses the same bit manipulation approach as hashint8 to ensure cross-type hash join compatibility, combining the high and low 32-bit halves of the 64-bit input value through XOR operations. The key difference is that it calls hash_uint32_extended instead of hash_uint32, passing the seed value as a second parameter to provide additional entropy in the hash computation.

## Parameters / Member Variables
- First argument (retrieved via PG_GETARG_INT64(0)): The 64-bit signed integer value to be hashed
- Second argument (retrieved via PG_GETARG_INT64(1)): A 64-bit seed value used to extend the hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: PostgreSQL macro to extract int64 arguments from function call (used twice)
  - [hash_uint32_extended](hash_uint32_extended.md): Extended 32-bit hash function that incorporates the seed value
- Called from (representative examples):
  - [time_hash_extended](../t/time_hash_extended.md): Extended hash function for time data type
  - [timetz_hash_extended](../t/timetz_hash_extended.md): Extended hash function for time with timezone data type
  - [pg_lsn_hash_extended](../p/pg_lsn_hash_extended.md): Extended hash function for PostgreSQL Log Sequence Number
  - [timestamp_hash_extended](../t/timestamp_hash_extended.md): Extended hash function for timestamp data type
  - [interval_hash_extended](../i/interval_hash_extended.md): Extended hash function for interval data type

## Notes and Other Information
- Maintains the same cross-type compatibility guarantees as hashint8 while providing extended hash functionality
- The comment 'Same approach as hashint8' indicates the intentional algorithmic consistency
- Uses identical bit manipulation logic to hashint8 but with extended hash computation
- Located in src/backend/access/hash/hashfunc.c:103-115
- Essential for hash-based operations requiring additional collision resistance
- The seed parameter enables better hash distribution in scenarios like hash joins with multiple hash functions