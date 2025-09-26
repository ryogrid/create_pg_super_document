# templ

## Location
[src/tools/pg_bsd_indent/lexi.c:60-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/lexi.c#L60-L141)

## Overview
The `templ` struct is a simple template structure used by the pg_bsd_indent tool to store C language keyword information for lexical analysis and code formatting.

## Definition
```c
struct templ {
    const char *rwd;
    int         rwcode;
};
```

## Detailed Description
The `templ` struct serves as a template for storing C language keywords and their associated classification codes in the PostgreSQL BSD-style code indentation tool. It is primarily used to create a lookup table (`specials` array) containing all recognized C keywords, operators, and language constructs that require special formatting treatment.

The struct is designed to be used with binary search operations, which requires the containing array to be sorted alphabetically by the `rwd` field. This design choice optimizes keyword lookup performance during lexical analysis of C source code.

The `specials` array contains 43 entries covering standard C keywords like `int`, `char`, `struct`, `if`, `while`, control flow statements, storage class specifiers, and other language constructs that the indenter needs to recognize and format appropriately.

## Parameters / Member Variables
- `rwd`: Pointer to a constant character string containing the reserved word/keyword text (e.g., "int", "while", "struct")
- `rwcode`: Integer classification code that determines how the keyword should be formatted and what parsing behavior it triggers (values range from 1-12, each representing different syntactic categories)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - `[lexi](../l/lexi.md)` function at src/tools/pg_bsd_indent/lexi.c:243 (declares pointer to struct templ)
  - `specials` array at src/tools/pg_bsd_indent/lexi.c:69 (array of struct templ)
  - `bsearch` function uses this struct via the `specials` array for keyword lookup

## Notes and Other Information
- The struct definition appears in src/tools/pg_bsd_indent/lexi.c:60-63
- The `specials` array must remain sorted alphabetically because it is searched using `bsearch()`
- The string field (`rwd`) must be the first member of the struct for proper binary search functionality
- Different `rwcode` values trigger different formatting behaviors:
  - Code 3: Structure-related keywords (struct, union, enum)
  - Code 4: Type specifiers (int, char, double, etc.)
  - Code 5: Control flow with parentheses (if, while, for)
  - Code 6: Control flow without parentheses (do, else)
  - Code 8: Case/default labels
  - Code 9: Jump statements (break, continue, return, goto)
  - Code 10: Storage class specifiers (auto, extern, register, static)
  - Code 11: typedef keyword
  - Code 12: Special keywords (continue, inline, restrict)
- This struct is part of the PostgreSQL source code formatting tools, not the core database engine