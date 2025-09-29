# pg_u_isblank

## Location
[src/common/unicode_category.c:255-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L255-L261)

## Overview
Determines whether a Unicode character is a blank character, specifically checking for tab characters and Unicode space separator characters.

## Definition
```c
bool pg_u_isblank(pg_wchar code)
```

## Detailed Description
This function implements Unicode blank character detection by checking if the input character is either:
1. A tab character (Unicode code point 0x09)
2. A Unicode space separator character (as determined by the Unicode category system)

The function follows the Unicode standard's definition of blank characters, which includes horizontal whitespace characters but excludes vertical whitespace like newlines. It provides a PostgreSQL-specific implementation for Unicode character classification that may be used when system locale functions are insufficient or unavailable.

## Parameters / Member Variables
- `code`: The Unicode character code point (pg_wchar) to test for blank character properties

## Dependencies
- Functions called/Symbols referenced:
  - [unicode_category](../u/unicode_category.md) (internal Unicode category determination function)
  - PG_U_CHARACTER_TAB (constant defining tab character code point 0x09)  
  - PG_U_SPACE_SEPARATOR (Unicode category constant for space separator characters)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (test function)
  - [pg_u_isprint](pg_u_isprint.md) (print character detection function)
  - [pg_unicode_category](pg_unicode_category.md) (Unicode category interface)

## Notes and Other Information
- Returns true for tab characters and Unicode space separators only
- Part of PostgreSQL's internal Unicode character classification system
- Located in src/common/unicode_category.c:255-261
- Designed to provide consistent Unicode character classification across different platforms and locales
- Does not include vertical whitespace characters like newlines in the blank character classification

## Simplified Source

```c
bool
pg_u_isblank(pg_wchar code)
{
    // Check for tab character OR Unicode space separator
    return code == PG_U_CHARACTER_TAB ||
           unicode_category(code) == PG_U_SPACE_SEPARATOR;
}
```