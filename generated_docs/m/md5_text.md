# md5_text

## Location
src/backend/utils/adt/cryptohashfuncs.c: 34 - 58

## Overview
Creates an MD5 hash of a text value and returns it as a hexadecimal string representation.

## Definition
```c
Datum md5_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQLs MD5() SQL function for text input. It takes a text value, computes its MD5 hash using the pg_md5_hash utility function, and returns the result as a hexadecimal string. The function handles variable-length text input by using PostgreSQLs varlena metadata system to determine the actual data length, then processes the raw text data to generate a 32-character hexadecimal MD5 hash.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to function arguments
- `in_text`: Input text value obtained via PG_GETARG_TEXT_PP(0)
- `len`: Length of the input text calculated using VARSIZE_ANY_EXHDR()
- `hexsum`: Character array to store the resulting hexadecimal hash (MD5_HASH_LEN + 1 bytes)
- `errstr`: Error string pointer for error reporting from pg_md5_hash

## Dependencies
- Functions called/Symbols referenced:
  - pg_md5_hash
  - cstring_to_text
  - PG_RETURN_TEXT_P
- Constants referenced:
  - MD5_HASH_LEN
- Called from (representative examples):
  - No direct references found (called via SQL function dispatcher)

## Notes and Other Information
- Located in src/backend/utils/adt/cryptohashfuncs.c:34-58
- Uses PostgreSQLs error reporting mechanism with ERRCODE_INTERNAL_ERROR for hash computation failures
- Part of PostgreSQLs cryptographic hash function suite accessible via SQL
- Returns a 32-character hexadecimal string representation of the MD5 hash
- Handles variable-length text input efficiently using varlena macros