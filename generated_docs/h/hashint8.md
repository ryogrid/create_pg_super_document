# hashint8

## Location
[src/backend/access/hash/hashfunc.c:83-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L83-L102)

## Overview
The hashint8 function computes a hash value for a 64-bit signed integer (int8) while maintaining compatibility with hash values produced by hashint4 and hashint2 for logically equal inputs.

## Definition
Datum hashint8(PG_FUNCTION_ARGS)

## Detailed Description
This function implements a specialized hash algorithm for 64-bit integers that ensures cross-type hash join compatibility with smaller integer types (int4 and int2). The algorithm works by combining the high and low 32-bit halves of the 64-bit value using XOR operation, with special handling for negative values to maintain consistency. For positive values, it XORs the low half with the high half. For negative values, it XORs the low half with the complement of the high half. This approach ensures that when a 32-bit or 16-bit integer is cast to 64-bit, the resulting hash value remains the same.

## Parameters / Member Variables
- First argument (retrieved via PG_GETARG_INT64(0)): The 64-bit signed integer value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: PostgreSQL macro to extract int64 argument from function call
  - [hash_uint32](hash_uint32.md): Core 32-bit hash function that performs the actual hash computation
- Called from (representative examples):
  - [time_hash](../t/time_hash.md): Hash function for time data type
  - [timetz_hash](../t/timetz_hash.md): Hash function for time with timezone data type
  - [pg_lsn_hash](../p/pg_lsn_hash.md): Hash function for PostgreSQL Log Sequence Number
  - [timestamp_hash](../t/timestamp_hash.md): Hash function for timestamp data type
  - [interval_hash](../i/interval_hash.md): Hash function for interval data type

## Notes and Other Information
- Critical for maintaining hash join compatibility across different integer types
- The algorithm ensures that casting smaller integers to int8 produces the same hash value
- Located in src/backend/access/hash/hashfunc.c:83-102
- Uses bit manipulation to combine high and low 32-bit portions of the 64-bit input
- The complement operation (~hihalf) for negative values ensures proper sign extension compatibility

## Simplified Source
```c
Datum hashint8(PG_FUNCTION_ARGS) {
    // Extract 64-bit integer and split into high/low 32-bit halves
    int64 val = PG_GETARG_INT64(0);
    uint32 lohalf = (uint32) val;
    uint32 hihalf = (uint32) (val >> 32);

    // XOR halves together for compatibility with smaller integer types
    // Use complement of high half for negative values
    lohalf ^= (val >= 0) ? hihalf : ~hihalf;

    return hash_uint32(lohalf);
}
```