# strcmp_type

## Location
src/tools/pg_bsd_indent/lexi.c: 142 - 159

## Overview
A static comparison function used by bsearch() to locate type names in a sorted array during lexical analysis in the pg_bsd_indent tool.

## Definition
static int strcmp_type(const void *e1, const void *e2)

## Detailed Description
This function serves as a comparison callback for the standard librarys bsearch() function. It implements the required comparison interface for searching through an array of type names. The function compares a search key (e1) with elements in a sorted array of string pointers (e2). This is part of the lexical analyzers type recognition mechanism in pg_bsd_indent, which helps format C code by identifying user-defined types.

## Parameters / Member Variables
- e1: Search key - a pointer to the string being searched for
- e2: Array element - a pointer to a pointer to a string from the sorted type names array

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
- Called from (representative examples):
  - [lexi](../l/lexi.md) (at src/tools/pg_bsd_indent/lexi.c:356)
  - [lexi](../l/lexi.md) (at src/tools/pg_bsd_indent/lexi.c:364)

## Notes and Other Information
- This function follows the standard comparison function interface required by bsearch()
- Returns negative, zero, or positive value based on the comparison result
- Used specifically for binary search operations on type name arrays
- Part of the pg_bsd_indent tools lexical analysis functionality