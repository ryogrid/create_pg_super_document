# hashcharextended

## Location
[src/backend/access/hash/hashfunc.c:53-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L53-L58)

## Overview
hashcharextended is an extended hash function for "char" and boolean data types in PostgreSQL that accepts an additional seed parameter for enhanced hash computation.

## Definition
```c
Datum hashcharextended(PG_FUNCTION_ARGS)
```

## Detailed Description
The hashcharextended function provides an extended version of the hashchar function, supporting seeded hash computation for single character ('char') and boolean data types. This function is part of PostgreSQL's extended hash function family that allows for hash value computation with a user-provided seed value, which is useful for applications requiring hash randomization or multi-level hashing schemes.

The function extracts both a character argument and a 64-bit integer seed from the function call context, then delegates the actual hashing to hash_uint32_extended after casting the character to a 32-bit integer.

## Parameters / Member Variables
- First argument: 'char' value accessed via PG_GETARG_CHAR(0)
- Second argument: int64 seed value accessed via PG_GETARG_INT64(1)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32_extended](hash_uint32_extended.md): Extended hash function for 32-bit unsigned integers with seed support
  - PG_GETARG_CHAR: Macro to extract char argument from function call context
  - PG_GETARG_INT64: Macro to extract 64-bit integer argument from function call context

- Called from (representative examples):
  - [JsonbHashScalarValueExtended](../J/JsonbHashScalarValueExtended.md): Used in JSONB hash operations for scalar values

## Notes and Other Information
- Extended version of hashchar function with seed parameter support
- Enables hash randomization and seeded hash computations
- Used in advanced hashing scenarios such as JSONB operations
- Returns a Datum containing the computed hash value
- Part of PostgreSQL's comprehensive extended hash function collection for various data types