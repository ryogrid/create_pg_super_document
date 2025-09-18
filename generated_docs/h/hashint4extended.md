# hashint4extended

## Location
src/backend/access/hash/hashfunc.c: 77 - 82

## Overview
The hashint4extended function is a PostgreSQL hash function that computes an extended hash value for a 32-bit signed integer (int4) using an additional 64-bit seed value.

## Definition


## Detailed Description
This function serves as a wrapper around the hash_uint32_extended function, providing extended hash functionality for PostgreSQL's int4 data type. Extended hash functions in PostgreSQL are used when additional entropy is needed for hash calculations, typically in scenarios like hash joins where collision resistance is important. The function takes both the integer value to be hashed and a 64-bit seed value to produce a more robust hash result.

## Parameters / Member Variables
- First argument (retrieved via PG_GETARG_INT32(0)): The 32-bit signed integer value to be hashed
- Second argument (retrieved via PG_GETARG_INT64(1)): A 64-bit seed value used to extend the hash computation

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32_extended](hash_uint32_extended.md): Core hash computation function
  - PG_GETARG_INT64: PostgreSQL macro to extract int64 argument from function call
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This function is part of PostgreSQL's hash index support infrastructure
- The extended hash variant provides better distribution properties when used with additional seed values
- Typically used internally by PostgreSQL's hash-based operations rather than being called directly by user code
- Located in src/backend/access/hash/hashfunc.c:77-82