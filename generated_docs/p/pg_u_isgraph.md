# pg_u_isgraph

## Location
src/common/unicode_category.c: 268 - 278

## Overview
Determines whether a Unicode character is a graphical character, meaning it is visible and printable but excludes whitespace characters.

## Definition
```c
bool pg_u_isgraph(pg_wchar code)
```

## Detailed Description
This function identifies Unicode graphical characters by excluding specific character categories that are not considered graphical. A character is considered graphical if it is not:
1. A control character (CC category)
2. A surrogate character (CS category) 
3. An unassigned character (CN category)
4. A whitespace character (as determined by pg_u_isspace)

The function uses Unicode category masks to efficiently check multiple character categories at once. Graphical characters are those that have a visible representation when displayed, excluding all forms of whitespace and control characters.

## Parameters / Member Variables
- `code`: The Unicode character code point (pg_wchar) to test for graphical character properties

## Dependencies
- Functions called/Symbols referenced:
  - unicode_category (internal Unicode category determination function)
  - pg_u_isspace (whitespace character detection function)
  - PG_U_CATEGORY_MASK (macro for category mask conversion)
  - PG_U_CC_MASK (control character category mask)
  - PG_U_CS_MASK (surrogate character category mask) 
  - PG_U_CN_MASK (unassigned character category mask)
- Called from (representative examples):
  - pg_wc_isgraph (regex locale wrapper function)
  - icu_test (test function)
  - pg_u_isprint (print character detection function)
  - pg_unicode_category (Unicode category interface)

## Notes and Other Information
- Returns true for visible, printable characters excluding whitespace
- Uses efficient bitwise operations with category masks for performance
- Part of PostgreSQL's internal Unicode character classification system
- Located in src/common/unicode_category.c:268-278
- Graphical characters form a subset of printable characters (printable = graphical + whitespace)
- Designed to provide consistent Unicode character classification across different platforms and locales