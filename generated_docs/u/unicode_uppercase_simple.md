# unicode_uppercase_simple

## Location
[src/common/unicode_case.c:45-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_case.c#L45-L68)

## Overview
Converts a Unicode codepoint to its uppercase equivalent using simple case mapping rules.

## Definition
```c
pg_wchar unicode_uppercase_simple(pg_wchar code)
```

## Detailed Description
This function performs simple Unicode case conversion to uppercase for a given Unicode codepoint. It looks up the codepoint in PostgreSQL's internal case mapping table and returns the corresponding uppercase character. If no case mapping exists for the given codepoint, the original codepoint is returned unchanged.

The function uses PostgreSQL's optimized case mapping table which provides dense storage for ASCII characters (codepoints < 0x80) for fast lookup, and sparse storage for higher codepoints that requires binary search.

## Parameters / Member Variables
- `code`: Input Unicode codepoint (pg_wchar) to be converted to uppercase

## Dependencies
- Functions called/Symbols referenced:
  - [find_case_map](../f/find_case_map.md)
  - pg_case_map (structure)
  - CaseUpper (enum value)
- Called from (representative examples):
  - [pg_wc_toupper](../p/pg_wc_toupper.md)
  - [icu_test_simple](../i/icu_test_simple.md)

## Notes and Other Information
- Returns the original codepoint if no uppercase mapping exists
- Uses PostgreSQL's internal Unicode case mapping table for conversion
- Part of PostgreSQL's Unicode handling infrastructure
- Located in src/common/unicode_case.c:45-52
- Commonly used in regular expression processing and text operations