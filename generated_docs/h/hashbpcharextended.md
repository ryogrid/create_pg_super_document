# hashbpcharextended

## Location
src/backend/utils/adt/varchar.c: 1053 - 1118

## Overview
Computes an extended hash value for BPCHAR data types using a provided seed value, ignoring trailing blanks and respecting collation settings for advanced hash-based operations.

## Definition
```c
Datum hashbpcharextended(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the extended version of hashbpchar that accepts an additional 64-bit seed parameter for more sophisticated hash-based operations. Like its counterpart, it computes hash values for BPCHAR data while ignoring trailing blanks to ensure semantic consistency. The extended version is particularly useful for advanced hashing scenarios such as parallel hash joins, distributed hashing, or when hash value variation across different contexts is needed.

The function follows the same collation-aware logic as hashbpchar, handling both deterministic and non-deterministic collations appropriately. For deterministic collations, it directly hashes the character data with the provided seed. For non-deterministic collations, it performs locale-specific transformation before hashing with the seed value.

The extended hash functionality enables more sophisticated hash distribution patterns and helps avoid hash collisions in complex query execution scenarios involving multiple hash operations.

## Parameters / Member Variables
- `key`: Input BPCHAR value to hash (extracted using PG_GETARG_BPCHAR_PP(0))
- `seed`: 64-bit seed value for extended hashing (extracted using PG_GETARG_INT64(1))
- `collid`: Collation ID for the hashing operation (from PG_GET_COLLATION())
- `keydata`: Pointer to the actual character data within the BPCHAR
- `keylen`: True length of the BPCHAR data (excluding trailing spaces)
- `mylocale`: Locale object for non-C collations (0 for C collation)
- `result`: Final extended hash value to return
- `bsize`: Buffer size needed for locale transformation
- `rsize`: Actual size returned by locale transformation
- `buf`: Buffer for holding transformed string data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (argument extraction macro)
  - PG_GETARG_INT64 (64-bit integer argument extraction)
  - PG_GET_COLLATION (gets current collation setting)
  - VARDATA_ANY (extracts character data from variable-length type)
  - [bcTruelen](../b/bcTruelen.md) (determines true length of BPCHAR, ignoring trailing spaces)
  - [lc_collate_is_c](../l/lc_collate_is_c.md) (checks if collation is C locale)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (creates locale object from collation ID)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md) (checks if locale has deterministic sorting)
  - [hash_any_extended](hash_any_extended.md) (computes extended hash value with seed)
  - [pg_strnxfrm](../p/pg_strnxfrm.md) (performs locale-aware string transformation)
  - [palloc](../p/palloc.md)/pfree (memory allocation/deallocation)
  - PG_FREE_IF_COPY (cleans up potentially toasted input)
- Called from (representative examples):
  - No direct references found (likely called through extended hash operator dispatch)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (argument extraction macro)
  - PG_GETARG_INT64 (64-bit integer argument extraction)
  - PG_GET_COLLATION (gets current collation setting)
  - VARDATA_ANY (extracts character data from variable-length type)
  - [bcTruelen](../b/bcTruelen.md) (determines true length of BPCHAR, ignoring trailing spaces)
  - [lc_collate_is_c](../l/lc_collate_is_c.md) (checks if collation is C locale)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (creates locale object from collation ID)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md) (checks if locale has deterministic sorting)
  - [hash_any_extended](hash_any_extended.md) (computes extended hash value with seed)
  - [pg_strnxfrm](../p/pg_strnxfrm.md) (performs locale-aware string transformation)
  - [palloc](../p/palloc.md)/pfree (memory allocation/deallocation)
  - PG_FREE_IF_COPY (cleans up potentially toasted input)
- Called from (representative examples):
  - No direct references found (likely called through extended hash operator dispatch)

## Notes and Other Information
- Extended version of hashbpchar with additional seed parameter for advanced hashing scenarios
- Essential for parallel hash operations, distributed hashing, and hash collision avoidance
- Maintains the same semantic consistency as hashbpchar regarding trailing blank handling
- Supports both deterministic and non-deterministic collations with appropriate transformation logic
- Used in sophisticated query execution plans where hash value variation is beneficial
- Preserves all memory safety and error handling characteristics of the base hashbpchar function
- The seed parameter enables hash value diversification across different contexts or parallel workers