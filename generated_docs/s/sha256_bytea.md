# sha256_bytea

## Location
src/backend/utils/adt/cryptohashfuncs.c: 148 - 155

## Overview
Computes the SHA-256 hash of a bytea (binary data) value and returns the result as binary data (bytea).

## Definition
```c
Datum sha256_bytea(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQLs SHA256() SQL function for bytea input. It serves as a simple wrapper around the cryptohash_internal function, specifically requesting SHA-256 hash computation. Unlike the MD5 functions that return hexadecimal strings, this function returns the raw binary hash digest as a bytea value. The SHA-256 algorithm produces a 32-byte (256-bit) hash digest and is widely used as a secure cryptographic hash function. This function is part of PostgreSQLs SHA-2 family of cryptographic hash functions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to function arguments
- `result`: The computed SHA-256 hash as a bytea value, obtained from cryptohash_internal
- Input bytea value is obtained via PG_GETARG_BYTEA_PP(0) and passed directly to cryptohash_internal

## Dependencies
- Functions called/Symbols referenced:
  - [cryptohash_internal](../c/cryptohash_internal.md)
  - PG_GETARG_BYTEA_PP
  - PG_RETURN_BYTEA_P
- Constants referenced:
  - PG_SHA256
- Called from (representative examples):
  - No direct references found (called via SQL function dispatcher)

## Notes and Other Information
- Located in src/backend/utils/adt/cryptohashfuncs.c:148-155
- Part of PostgreSQLs SHA-2 cryptographic hash function family
- Returns raw binary data (32 bytes for SHA-256) rather than hexadecimal string representation
- Very simple wrapper function that delegates all hash computation to cryptohash_internal
- SHA-256 produces a 256-bit (32-byte) digest and is one of the most commonly used secure hash algorithms
- Accessible via SQL as the SHA256() function for bytea arguments
- Uses PostgreSQLs standard function calling conventions and return mechanisms
- Provides better security than older hash algorithms like MD5 and SHA-1