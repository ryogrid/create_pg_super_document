# get_code_entry

## Location
[src/common/unicode_norm.c:72-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L72-L111)

## Overview
Retrieves the decomposition entry for a given Unicode codepoint from the decomposition lookup table using either a perfect hash function (backend) or binary search (frontend).

## Definition
```c
static const pg_unicode_decomposition * get_code_entry(pg_wchar code)
```

## Detailed Description
`get_code_entry` is a key function in PostgreSQL Unicode normalization that locates decomposition information for a specific Unicode codepoint. The function has two different implementations depending on the compilation context:

- **Backend version**: Uses a perfect hash function for O(1) lookup performance. It computes a hash from the codepoint (in network byte order) and directly indexes into the decomposition table.
- **Frontend version**: Uses binary search with the `conv_compare` function for O(log n) lookup performance.

The perfect hash approach in the backend provides optimal performance for database operations, while the simpler binary search in the frontend version reduces code complexity for utility programs.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to look up in the decomposition table

## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_decompinfo](../p/pg_unicode_decompinfo.md) (backend only)
  - pg_hton32 (backend only)
  - bsearch (frontend only)
  - [conv_compare](../c/conv_compare.md) (frontend only)
  - lengthof (frontend only)
  - [pg_unicode_decomposition](../p/pg_unicode_decomposition.md) (structure type)
- Called from (representative examples):
  - [get_canonical_class](get_canonical_class.md)
  - [get_decomposed_size](get_decomposed_size.md)
  - [decompose_code](../d/decompose_code.md)

## Notes and Other Information
- This is a static function, accessible only within unicode_norm.c
- Returns NULL if no decomposition entry exists for the given codepoint
- The backend version uses UnicodeDecompInfo for the perfect hash lookup
- The frontend version searches UnicodeDecompMain using binary search
- The hash function uses network byte order (big-endian) for consistent results across platforms
- Perfect hash eliminates collisions, requiring only a single codepoint comparison for verification