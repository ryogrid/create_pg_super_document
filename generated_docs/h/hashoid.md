# hashoid

## Location
[src/backend/access/hash/hashfunc.c:116-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L116-L121)

## Overview
The hashoid function computes a hash value for PostgreSQL's OID (Object Identifier) data type by treating it as a 32-bit unsigned integer.

## Definition
Datum hashoid(PG_FUNCTION_ARGS)

## Detailed Description
This function provides hash functionality for PostgreSQL's OID data type, which is used internally to uniquely identify database objects such as tables, functions, and types. Since OIDs are internally represented as 32-bit unsigned integers, the function simply extracts the OID value using PG_GETARG_OID and casts it to uint32 before passing it to the standard 32-bit hash function. This straightforward approach ensures consistent and efficient hashing of OID values for use in hash-based operations like hash indexes and hash joins.

## Parameters / Member Variables
- First argument (retrieved via PG_GETARG_OID(0)): The OID value to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32](hash_uint32.md): Core 32-bit hash function that performs the actual hash computation
  - PG_GETARG_OID: PostgreSQL macro to extract OID argument from function call (implicitly used)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- OIDs are 32-bit unsigned integers used as unique identifiers within PostgreSQL
- This function enables OID columns to be used with hash indexes and hash-based query operations
- The simple cast from OID to uint32 is safe since they have the same underlying representation
- Located in src/backend/access/hash/hashfunc.c:116-121
- Part of PostgreSQL's comprehensive hash function support for built-in data types

## Simplified Source
```c
Datum hashoid(PG_FUNCTION_ARGS) {
    // Extract OID argument and cast to 32-bit unsigned for hashing
    return hash_uint32((uint32) PG_GETARG_OID(0));
}
```