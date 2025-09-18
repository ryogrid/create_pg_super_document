# directives

## Location
src/tools/pg_bsd_indent/indent.c: 1154 - 1202

## Overview
The  struct is a local data structure used within pg_bsd_indent's main function to recognize and validate C preprocessor directives during code formatting.

## Definition


## Detailed Description
The  struct is a simple data structure that pairs each recognized C preprocessor directive with its string length for efficient string comparison. It is defined locally within the main parsing loop of pg_bsd_indent (PostgreSQL's BSD-style code indenter) and is used to create a lookup table of valid preprocessor directives.

The struct is instantiated as an array called  containing the following preprocessor directives:
-  (size: 7)
-  (size: 6) 
-  (size: 5)
-  (size: 4)
-  (size: 5)
-  (size: 6)

This lookup mechanism allows pg_bsd_indent to validate preprocessor directives and issue diagnostics for unrecognized ones, ensuring proper code formatting around preprocessor statements.

## Parameters / Member Variables
- : Integer representing the length of the directive string, used for efficient strncmp operations
- : Constant character pointer to the directive name string (without the '#' prefix)

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (for directive string comparison)
  - [diag2](diag2.md) (for error reporting of unrecognized directives)
- Called from (representative examples):
  - Used within the main() function's preprocessor parsing logic (preesc case)

## Notes and Other Information
- This is a local struct definition used only within the main function of pg_bsd_indent
- The struct enables efficient recognition of standard C preprocessor directives during code indentation
- The size field eliminates the need to call strlen() during string comparison operations
- Part of PostgreSQL's BSD-style code formatting tool, not the core database engine
- No direct references to this symbol from other parts of the codebase as it's locally scoped