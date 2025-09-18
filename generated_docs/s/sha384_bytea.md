# sha384_bytea

## Location
src/backend/utils/adt/cryptohashfuncs.c: 156 - 163

## Overview
This function computes the SHA-384 cryptographic hash of bytea (binary data) input and returns the result as a bytea value.

## Definition
```c
Datum sha384_bytea(PG_FUNCTION_ARGS)
```

## Detailed Description
The `sha384_bytea` function is a PostgreSQL built-in function that provides SHA-384 hashing capability for binary data (bytea type). It serves as a wrapper around the internal `cryptohash_internal` function, specifically configured to compute SHA-384 hashes. The function follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro and returns a `Datum` type that can be converted back to PostgreSQL's bytea type.

The implementation is straightforward: it extracts the input bytea argument, passes it to the internal hash computation function with the SHA-384 algorithm identifier, and returns the resulting hash as a bytea value.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the input bytea data to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - `[cryptohash_internal](../c/cryptohash_internal.md)`: Internal utility function that performs the actual SHA-384 hash computation
  - `PG_SHA384`: Constant identifier for the SHA-384 algorithm
  - `PG_GETARG_BYTEA_PP`: Macro to extract bytea argument from function call
  - `PG_RETURN_BYTEA_P`: Macro to return bytea result from function
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's cryptographic hash function family located in `src/backend/utils/adt/cryptohashfuncs.c`
- The function produces a 48-byte (384-bit) hash output, which is the standard length for SHA-384
- As with all cryptographic hash functions, the output is deterministic - the same input will always produce the same hash
- This function is typically exposed to SQL users as a built-in function for computing SHA-384 hashes of binary data