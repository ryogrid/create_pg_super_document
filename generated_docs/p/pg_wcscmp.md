# pg_wcscmp

## Location
src/common/unicode/norm_test.c: 44 - 59

## Overview
A static utility function that compares two PostgreSQL wide character strings lexicographically, similar to the standard library's wcscmp function.

## Definition


## Detailed Description
The pg_wcscmp function performs a lexicographic comparison of two wide character strings represented as pg_wchar arrays. It iterates through both strings character by character, comparing the Unicode code points directly. The function returns a negative value if the first string is lexicographically smaller, a positive value if it's larger, or zero if the strings are identical. This is a test utility function specifically used in Unicode normalization testing within PostgreSQL's common library.

## Parameters / Member Variables
- : Pointer to the first null-terminated pg_wchar string to compare
- : Pointer to the second null-terminated pg_wchar string to compare

## Dependencies
- Functions called/Symbols referenced: (none - uses only basic pointer arithmetic and comparison)
- Called from (representative examples):
  - main (in norm_test.c)

## Notes and Other Information
- This function is declared as static, making it local to the norm_test.c file
- It assumes both input strings are null-terminated
- The comparison is performed at the Unicode code point level, not considering any locale-specific collation rules
- Used specifically for testing Unicode normalization functionality in PostgreSQL
- Returns -1, 0, or 1 following standard C library comparison function conventions