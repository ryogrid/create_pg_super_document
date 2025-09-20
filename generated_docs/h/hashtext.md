# hashtext

## Location
[src/backend/access/hash/hashfunc.c:267-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L267-L322)

## Overview
A PostgreSQL hash function that computes hash values for text data types with proper collation-aware handling.

## Definition

```c
Datum
hashtext(PG_FUNCTION_ARGS)
```
## Detailed Description
The hashtext function generates hash values for text data types in PostgreSQL, with careful consideration for collation rules. It first determines the collation to use, then chooses between two hashing strategies: for deterministic locales, it directly hashes the text data; for non-deterministic locales, it transforms the text using pg_strnxfrm() to ensure collation-equivalent strings produce the same hash value. This function is essential for hash-based operations like hash joins and hash indexes on text columns.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The text value to be hashed (text*)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - pg_locale_t
  - [lc_collate_is_c](../l/lc_collate_is_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md)
  - [hash_any](hash_any.md)
  - [pg_strnxfrm](../p/pg_strnxfrm.md)

- Called from (representative examples):
  - texthashfast

## Notes and Other Information
- Requires explicit collation specification; throws error if collation cannot be determined
- Uses locale transformation for non-C locales to ensure collation-equivalent strings hash to the same value
- Preserves legacy behavior by including the terminating NUL character in transformed strings
- Properly handles toasted inputs with memory cleanup using PG_FREE_IF_COPY
- Located at src/backend/access/hash/hashfunc.c:267-322