# sha224_bytea

## Location
[src/backend/utils/adt/cryptohashfuncs.c:140-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cryptohashfuncs.c#L140-L147)

## Overview
Computes the SHA-224 hash of a bytea (binary data) value and returns the result as binary data (bytea).

## Definition
```c
Datum sha224_bytea(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQLs SHA224() SQL function for bytea input. It serves as a simple wrapper around the cryptohash_internal function, specifically requesting SHA-224 hash computation. Unlike the MD5 functions that return hexadecimal strings, this function returns the raw binary hash digest as a bytea value. The SHA-224 algorithm produces a 28-byte (224-bit) hash digest. This function is part of PostgreSQLs SHA-2 family of cryptographic hash functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to function arguments
- `result`: The computed SHA-224 hash as a bytea value, obtained from cryptohash_internal
- Input bytea value is obtained via PG_GETARG_BYTEA_PP(0) and passed directly to cryptohash_internal

## Dependencies
- Functions called/Symbols referenced:
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - PG_GETARG_BYTEA_PP
  - PG_RETURN_BYTEA_P
- Constants referenced:
  - PG_SHA224
- Called from (representative examples):
  - No direct references found (called via SQL function dispatcher)

## Notes and Other Information
- Located in src/backend/utils/adt/cryptohashfuncs.c:140-147
- Part of PostgreSQLs SHA-2 cryptographic hash function family
- Returns raw binary data (28 bytes for SHA-224) rather than hexadecimal string representation
- Very simple wrapper function that delegates all hash computation to cryptohash_internal
- SHA-224 produces a 224-bit (28-byte) digest, shorter than SHA-256 but from the same SHA-2 family
- Accessible via SQL as the SHA224() function for bytea arguments
- Uses PostgreSQLs standard function calling conventions and return mechanisms