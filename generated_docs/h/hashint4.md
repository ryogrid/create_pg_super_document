# hashint4

## Location
[src/backend/access/hash/hashfunc.c:71-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L71-L76)

## Overview
hashint4 is a hash function for the int4 (32-bit integer) data type in PostgreSQL, used to compute hash values for hash indexes and hash joins.

## Definition
```c
Datum hashint4(PG_FUNCTION_ARGS)
```

## Detailed Description
The hashint4 function provides a hash implementation for the int4 (32-bit signed integer) data type in PostgreSQL. It serves as a datatype-specific hash function that supports both hash indexes and hash joins. The function extracts a 32-bit integer argument using the PostgreSQL function call interface and directly passes it to the generic hash_uint32 function without type casting, as both the input and the underlying hash function operate on 32-bit values.

This function is part of PostgreSQL's comprehensive collection of datatype-specific hash functions and may also be utilized by catcache operations without direct connection to hash indexes.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32](hash_uint32.md): Generic hash function for 32-bit unsigned integers

- Called from (representative examples):
  - No direct references found in the analyzed codebase (likely referenced through function pointers in system catalogs)

## Notes and Other Information
- Handles int4 (integer) data type which represents 32-bit signed integers in PostgreSQL
- Part of PostgreSQL's comprehensive datatype-specific hash function collection
- Returns a Datum (PostgreSQL's generic data type) containing the hash value
- No type casting required since the input is already 32-bit, matching the underlying hash function's expected input
- Designed to be called through PostgreSQL's function manager (fmgr) interface
- The int4 data type is the most commonly used integer type in PostgreSQL applications

## Simplified Source
```c
Datum hashint4(PG_FUNCTION_ARGS) {
    // Extract 32-bit integer argument and hash it directly
    return hash_uint32(PG_GETARG_INT32(0));
}
```