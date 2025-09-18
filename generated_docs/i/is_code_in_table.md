# is_code_in_table

## Location
src/common/saslprep.c: 987 - 1006

## Overview
A static utility function that determines whether a given Unicode codepoint exists within a sorted table of codepoint ranges using binary search.

## Definition
```c
static bool is_code_in_table(pg_wchar code, const pg_wchar *map, int mapsize)
```

## Detailed Description
This function performs efficient lookups to determine if a Unicode codepoint falls within any of the ranges stored in a sorted table. The table is organized as pairs of values representing [lower_bound, upper_bound] ranges. The function first performs a quick bounds check against the entire table, then uses binary search for precise range matching.

The function is optimized for performance with an initial bounds check that can quickly eliminate codepoints that fall outside the entire table range. For codepoints that might be in the table, it uses bsearch() with the codepoint_range_cmp comparison function to efficiently locate the appropriate range.

This is a core component of Unicode character classification in SASL string preparation, used to check if codepoints belong to specific Unicode categories or character sets.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to search for
- `map`: Pointer to a sorted array of codepoint ranges, organized as pairs [lower, upper]
- `mapsize`: Total number of elements in the map array (must be even, as ranges come in pairs)

## Dependencies
- Functions called/Symbols referenced:
  - codepoint_range_cmp (used as bsearch callback)
  - bsearch (standard library binary search)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - IS_CODE_IN_TABLE (macro wrapper)

## Notes and Other Information
- This is a static function local to src/common/saslprep.c
- Includes an assertion that mapsize must be even (since ranges are stored as pairs)
- Uses an optimization to quickly reject codepoints outside the table bounds
- Critical for SASL string preparation Unicode processing performance
- The map parameter must be sorted for binary search to work correctly