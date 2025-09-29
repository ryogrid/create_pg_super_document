# pg_u_isprint

## Location
[src/common/unicode_category.c:279-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L279-L289)

## Overview
Determines whether a Unicode character is printable, meaning it can be displayed or printed and includes both graphical characters and whitespace characters.

## Definition
```c
bool pg_u_isprint(pg_wchar code)
```

## Detailed Description
This function identifies Unicode printable characters by combining graphical characters (visible symbols) and blank characters (spaces and tabs). A character is considered printable if it:
1. Is NOT a control character (PG_U_CONTROL category)
2. Is either a graphical character (as determined by pg_u_isgraph) OR a blank character (as determined by pg_u_isblank)

This encompasses all characters that would typically appear in printed text, including letters, numbers, punctuation, symbols, spaces, and tabs, but excludes non-printing control characters like null, line feeds, and other formatting controls.

## Parameters / Member Variables
- `code`: The Unicode character code point (pg_wchar) to test for printable character properties

## Dependencies
- Functions called/Symbols referenced:
  - [unicode_category](../u/unicode_category.md) (internal Unicode category determination function)
  - [pg_u_isgraph](pg_u_isgraph.md) (graphical character detection function)
  - [pg_u_isblank](pg_u_isblank.md) (blank character detection function)
  - [pg_unicode_category](pg_unicode_category.md) (Unicode category type)
  - PG_U_CONTROL (Unicode category constant for control characters)
- Called from (representative examples):
  - [pg_wc_isprint](pg_wc_isprint.md) (regex locale wrapper function)
  - [icu_test](../i/icu_test.md) (test function)
  - [pg_unicode_category](pg_unicode_category.md) (Unicode category interface)

## Notes and Other Information
- Returns true for all characters suitable for display or printing
- Combines graphical and blank characters while excluding control characters
- Part of PostgreSQL's internal Unicode character classification system
- Located in src/common/unicode_category.c:279-289
- Printable characters = graphical characters + blank characters - control characters
- Provides platform-independent Unicode character classification for PostgreSQL text processing
- Essential for text validation and display operations within PostgreSQL

## Simplified Source

```c
bool
pg_u_isprint(pg_wchar code) {
    pg_unicode_category category = unicode_category(code);

    // Exclude control characters
    if (category == PG_U_CONTROL) {
        return false;
    }

    // Include graphical characters and blank characters
    return pg_u_isgraph(code) || pg_u_isblank(code);
}
```