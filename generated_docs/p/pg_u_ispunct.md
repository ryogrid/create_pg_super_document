# pg_u_ispunct

## Location
[src/common/unicode_category.c:290-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L290-L310)

## Overview
Determines whether a Unicode character is a punctuation character, with different behavior depending on whether POSIX or Unicode classification is requested.

## Definition
```c
bool pg_u_ispunct(pg_wchar code, bool posix)
```

## Detailed Description
This function identifies Unicode punctuation characters with two different classification modes:

**POSIX Mode (posix = true):**
- Excludes alphabetic characters first (calls pg_u_isalpha)
- Includes both punctuation (P category) AND symbol (S category) characters
- Follows POSIX locale conventions where symbols are considered punctuation

**Unicode Mode (posix = false):**
- Includes only punctuation category (P category) characters
- Follows strict Unicode categorization where symbols and punctuation are distinct

The function uses Unicode category masks for efficient category checking. This dual-mode approach allows PostgreSQL to provide both POSIX-compatible behavior for legacy applications and strict Unicode behavior for modern text processing.

## Parameters / Member Variables
- `code`: The Unicode character code point (pg_wchar) to test for punctuation character properties
- `posix`: Boolean flag determining classification mode - true for POSIX behavior (punctuation + symbols), false for Unicode-only behavior (punctuation only)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_u_isalpha](pg_u_isalpha.md) (alphabetic character detection function, used in POSIX mode)
  - [unicode_category](../u/unicode_category.md) (internal Unicode category determination function)
  - PG_U_CATEGORY_MASK (macro for category mask conversion)
  - PG_U_P_MASK (punctuation character category mask)
  - PG_U_S_MASK (symbol character category mask, used in POSIX mode)
- Called from (representative examples):
  - [pg_wc_ispunct](pg_wc_ispunct.md) (regex locale wrapper function)
  - [icu_test](../i/icu_test.md) (test function)
  - [pg_unicode_category](pg_unicode_category.md) (Unicode category interface)

## Notes and Other Information
- Supports both POSIX and Unicode punctuation classification standards
- POSIX mode treats symbols as punctuation for compatibility with traditional locale behavior
- Unicode mode strictly follows Unicode category definitions
- Uses efficient bitwise operations with category masks for performance
- Part of PostgreSQL's internal Unicode character classification system
- Located in src/common/unicode_category.c:290-310
- Essential for text parsing, regex operations, and locale-aware text processing
- The dual-mode design ensures compatibility across different character classification standards