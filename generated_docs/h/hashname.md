# hashname

## Location
[src/backend/access/hash/hashfunc.c:250-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L250-L257)

## Overview
Computes a hash value for a PostgreSQL Name data type by hashing its string content.

## Definition

```c
Datum
hashname(PG_FUNCTION_ARGS)
```
## Detailed Description
This function generates a hash value for a PostgreSQL Name data type. The Name type is a fixed-length string type used internally by PostgreSQL for storing identifiers like table names, column names, and other database object names. The function extracts the string content using NameStr macro and computes a hash based on the actual string length (excluding any null padding).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The Name data type argument to be hashed
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extract Name argument from function call
  - NameStr: Macro to extract string from Name data type
  - [hash_any](hash_any.md): Generic hash function for binary data
  - strlen: Calculate string length
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for Name data types
- Uses strlen to determine the actual string length, ignoring null padding in the Name type
- Name is a fixed-size data type (NAMEDATALEN bytes) but may contain shorter strings
- Commonly used for hashing database object identifiers in system catalogs
- The hash only includes the actual string content, not the padding
- Located in src/backend/access/hash/hashfunc.c:250-257

## Simplified Source
```c
Datum hashname(PG_FUNCTION_ARGS) {
    char *key = NameStr(*PG_GETARG_NAME(0));

    // Hash the actual string content based on its length
    return hash_any((unsigned char *) key, strlen(key));
}
```