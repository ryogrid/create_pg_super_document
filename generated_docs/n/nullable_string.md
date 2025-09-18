# nullable_string

## Location
[src/backend/nodes/readfuncs.c:182-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L182-L202)

## Overview
A static helper function in the PostgreSQL node deserialization system that processes string tokens during node reading operations, handling NULL values, empty strings, and escaped characters appropriately.

## Definition
```c
static char *nullable_string(const char *token, int length)
```

## Detailed Description
The `nullable_string` function is a critical utility in PostgreSQL's node deserialization process that converts string tokens back to their original string representations. It handles three specific cases:

1. **NULL values**: When `outToken` emits `<>` for NULL values, `pg_strtok` converts this to an empty string (length 0), which this function correctly interprets as NULL.

2. **Empty strings**: When `outToken` emits `""` for actual empty strings, this function detects the two-character quoted empty string and returns a proper empty string via `pstrdup("")`.

3. **Regular strings**: For all other strings, it removes protective backslashes that were added by `outToken` during serialization by calling `debackslash`.

This function is part of the node deserialization machinery that allows PostgreSQL to reconstruct complex data structures from their serialized text representations.

## Parameters / Member Variables
- `token`: Pointer to the string token to be processed
- `length`: Length of the token string

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md) (for duplicating empty strings)
  - [debackslash](../d/debackslash.md) (for removing escape sequences)
- Called from (representative examples):
  - `READ_STRING_FIELD` (macro for reading string fields)
  - [_readExtensibleNode](../r/_readExtensibleNode.md) (for extensible node deserialization)

## Notes and Other Information
- This is a static function, only accessible within the readfuncs.c compilation unit
- Works in conjunction with `outToken` from outfuncs.c to ensure proper round-trip serialization/deserialization
- The function assumes that the calling context has already validated the token and length parameters
- Part of PostgreSQL's broader node system used for query plans, parse trees, and other internal data structures