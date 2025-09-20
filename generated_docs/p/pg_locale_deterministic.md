# pg_locale_deterministic

## Location
[src/backend/utils/adt/pg_locale.c:1554-1573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1554-L1573)

## Overview
A function that determines whether a given PostgreSQL locale produces deterministic collation results, which is essential for operations requiring consistent ordering and hashing.

## Definition

```c
struct pg_locale_struct result;
```
## Detailed Description
This function checks whether a PostgreSQL locale is deterministic. A deterministic locale ensures that string comparison and collation operations always produce the same results for identical inputs, which is crucial for operations like indexing, hashing, and ORDER BY clauses. The function implements a simple but important rule: the default locale (represented by NULL) is always considered deterministic, while custom locales have their deterministic property explicitly stored and returned.

## Parameters / Member Variables
- `locale`: A pg_locale_t structure representing the locale to check, or NULL for the default locale

## Dependencies
- Functions called/Symbols referenced:
  - pg_locale_t (data type used in function signature and parameter)
- Called from (representative examples):
  - [hashtext](../h/hashtext.md) (at line 283)
  - [hashtextextended](../h/hashtextextended.md) (at line 339)
  - [pg_set_regex_collation](pg_set_regex_collation.md) (at line 259)
  - [GenericMatchText](../G/GenericMatchText.md) (at line 156)
  - [varstr_cmp](../v/varstr_cmp.md) (at line 1578)
  - [texteq](../t/texteq.md) (at line 1633)
  - [varstrfastcmp_locale](../v/varstrfastcmp_locale.md) (at line 2222)

## Notes and Other Information
- The default locale (NULL parameter) is always considered deterministic by design
- This function is widely used throughout PostgreSQL's text processing and comparison operations
- Deterministic behavior is crucial for hash-based operations and index consistency
- Non-deterministic locales can cause issues with hash joins, hash aggregation, and unique indexes
- The deterministic flag is stored within the pg_locale_t structure for custom locales