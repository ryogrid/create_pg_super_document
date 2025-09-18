# hashvarlena

## Location
src/backend/access/hash/hashfunc.c: 383 - 397

## Overview
A general-purpose hash function for variable-length data types where all bits are significant for equality comparison.

## Definition


## Detailed Description
The hashvarlena function provides hash computation for any variable-length (varlena) data type where there are no non-significant bits - meaning that distinct bit patterns never compare as equal. This function directly hashes the data content without any transformation, making it suitable for data types like bytea, varchar (in C locale), and other binary or simple text data where byte-for-byte comparison determines equality.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The varlena value to be hashed (struct varlena*)

## Dependencies
- Functions called/Symbols referenced:
  - varlena
  - PG_GETARG_VARLENA_PP
  - hash_any

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Suitable only for data types where all bits are significant for equality
- Does not perform any collation or locale-specific transformations
- More efficient than text-specific hash functions for binary data
- Handles toasted inputs properly with PG_FREE_IF_COPY memory cleanup
- Located at src/backend/access/hash/hashfunc.c:383-397