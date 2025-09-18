# hashbpchar

## Location
[src/backend/utils/adt/varchar.c:996-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L996-L1052)

## Overview
Computes a hash value for BPCHAR (blank-padded CHAR) data types, ignoring trailing blanks and respecting collation settings for consistent hash-based operations.

## Definition
```c
Datum hashbpchar(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a specialized hash function for BPCHAR data types that is crucial for hash-based operations like hash joins, hash aggregation, and hash indexes. The function is designed to ignore trailing blanks during hashing, ensuring that semantically equivalent BPCHAR values (which may differ only in trailing spaces) produce the same hash value.

The function handles both deterministic and non-deterministic collations. For deterministic collations (like C locale), it directly hashes the character data. For non-deterministic collations, it uses locale-specific transformation (pg_strnxfrm) to create a canonical form before hashing, ensuring that strings that compare as equal under the collation rules also hash to the same value.

The function includes proper error handling for missing collation information and memory management for both transformed strings and potentially toasted (compressed/out-of-line) input values.

## Parameters / Member Variables
- `key`: Input BPCHAR value to hash (extracted using PG_GETARG_BPCHAR_PP(0))
- `collid`: Collation ID for the hashing operation (from PG_GET_COLLATION())
- `keydata`: Pointer to the actual character data within the BPCHAR
- `keylen`: True length of the BPCHAR data (excluding trailing spaces)
- `mylocale`: Locale object for non-C collations (0 for C collation)
- `result`: Final hash value to return
- `bsize`: Buffer size needed for locale transformation
- `rsize`: Actual size returned by locale transformation
- `buf`: Buffer for holding transformed string data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (argument extraction macro)
  - PG_GET_COLLATION (gets current collation setting)
  - VARDATA_ANY (extracts character data from variable-length type)
  - [bcTruelen](../b/bcTruelen.md) (determines true length of BPCHAR, ignoring trailing spaces)
  - [lc_collate_is_c](../l/lc_collate_is_c.md) (checks if collation is C locale)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (creates locale object from collation ID)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md) (checks if locale has deterministic sorting)
  - [hash_any](hash_any.md) (computes hash value from byte array)
  - [pg_strnxfrm](../p/pg_strnxfrm.md) (performs locale-aware string transformation)
  - [palloc](../p/palloc.md)/pfree (memory allocation/deallocation)
  - PG_FREE_IF_COPY (cleans up potentially toasted input)
- Called from (representative examples):
  - No direct references found (likely called through hash operator dispatch)

## Notes and Other Information
- Critical for hash-based query operations on BPCHAR columns (hash joins, hash aggregation, hash indexes)
- Must produce consistent hash values for strings that compare as equal under BPCHAR semantics
- Handles both simple C collation (direct hashing) and complex locale-specific collations (transformation-based hashing)
- Includes protection against indeterminate collation errors with helpful error messages
- Memory-safe handling of both regular and toasted (out-of-line) BPCHAR values
- The transformation approach for non-deterministic collations ensures hash consistency across different string representations that are collation-equivalent
- Preserves legacy behavior by including the NUL terminator in transformed string hashes