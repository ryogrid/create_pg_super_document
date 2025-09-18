# check_hash_func_signature

## Location
src/backend/access/hash/hashvalidate.c: 275 - 351

## Overview
The check_hash_func_signature function is a static helper function that validates hash function signatures specifically for PostgreSQL hash operator classes, including custom validation rules to accommodate built-in hash opclass implementation quirks.

## Definition
```c
static bool check_hash_func_signature(Oid funcid, int16 amprocnum, Oid argtype)
```

## Detailed Description
This function provides custom signature validation for hash access method support functions, implementing special validation logic beyond the standard amproc signature checking. It handles two types of hash functions:

1. **Standard Hash Functions (HASHSTANDARD_PROC)**: Expected to return int4 (32-bit hash) and take 1 argument
2. **Extended Hash Functions (HASHEXTENDED_PROC)**: Expected to return int8 (64-bit hash) and take 2 arguments (data + 64-bit salt)

The function implements special compatibility rules for built-in PostgreSQL hash operator classes that use "compatible but different" data types. These exceptions allow certain hash functions to be used with related data types that are physically compatible but not formally binary coercible, such as using hashint4() for dates, XIDs, and command IDs.

## Parameters / Member Variables
- `funcid`: The OID of the hash function to validate
- `amprocnum`: The support procedure number (HASHSTANDARD_PROC or HASHEXTENDED_PROC) 
- `argtype`: The expected argument data type for the hash function

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - IsBinaryCoercible
  - ReleaseSysCache
  - GETSTRUCT
- Called from (representative examples):
  - hashvalidate (during operator class validation)

## Notes and Other Information
- This is a custom implementation needed because standard amproc signature validation is too strict for PostgreSQL's built-in hash operator classes.
- The function includes hardcoded exceptions for specific function/type combinations that are known to be safe despite not passing formal coercibility checks.
- Extended hash functions must accept a 64-bit salt as their second parameter.
- The allowed exceptions include: hashint4() for dates/XIDs/CIDs, hashint8() for XID8, timestamp_hash() for timestamptz, hashchar() for boolean, and hashvarlena() for bytea.
- Located in src/backend/access/hash/hashvalidate.c:275-351.