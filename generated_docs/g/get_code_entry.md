# get_code_entry

## Location
src/common/unicode_norm.c: 72 - 111

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
  - pg_unicode_decompinfo (backend only)
  - pg_hton32 (backend only)
  - bsearch (frontend only)
  - conv_compare (frontend only)
  - lengthof (frontend only)
  - pg_unicode_decomposition (structure type)
- Called from (representative examples):
  - get_canonical_class
  - get_decomposed_size
  - decompose_code

## Notes and Other Information
- This is a static function, accessible only within unicode_norm.c
- Returns NULL if no decomposition entry exists for the given codepoint
- The backend version uses UnicodeDecompInfo for the perfect hash lookup
- The frontend version searches UnicodeDecompMain using binary search
- The hash function uses network byte order (big-endian) for consistent results across platforms
- Perfect hash eliminates collisions, requiring only a single codepoint comparison for verification