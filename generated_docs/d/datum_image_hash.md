# datum_image_hash

## Location
src/backend/utils/adt/datum.c: 338 - 396

## Overview
Generates a hash value based on the binary representation of a PostgreSQL Datum, operating on the actual bits rather than the logical value of the data.

## Definition


## Detailed Description
The  function provides binary-level hashing for PostgreSQL Datums, which is essential for operations that need to distinguish between different binary representations of logically equivalent values. Unlike type-specific hash functions that operate on logical values, this function operates directly on the memory representation of the data.

The function handles different data storage formats based on the  and  parameters:
- For pass-by-value types, it hashes the Datum value directly
- For fixed-length pass-by-reference types, it hashes the referenced data
- For variable-length types (typLen = -1), it handles TOAST decompression and hashes the variable data portion
- For null-terminated strings (typLen = -2), it hashes the string including the null terminator

This function is particularly useful in scenarios like query memoization where exact binary equivalence is required rather than logical equivalence.

## Parameters / Member Variables
- : The Datum value to hash
- : Boolean indicating whether the type is passed by value or by reference
- : Length specification for the type (positive for fixed length, -1 for variable length, -2 for null-terminated strings)

## Dependencies
- Functions called/Symbols referenced:
  - hash_bytes (core hashing function)
  - toast_raw_datum_size (gets size of potentially TOASTed data)
  - PG_DETOAST_DATUM_PACKED (decompresses TOASTed data)
  - DatumGetCString (extracts C string from Datum)
  - VARDATA_ANY (gets variable data portion)
  - pfree (memory deallocation)
- Called from (representative examples):
  - MemoizeHash_hash (in query memoization)

## Notes and Other Information
- The function carefully handles memory management for detoasted values, only freeing memory when a copy was made
- Error handling is provided for unexpected typLen values
- The binary-level approach makes this function sensitive to platform-specific byte ordering and padding
- This function is declared in src/include/utils/datum.h and is part of the public PostgreSQL utility API