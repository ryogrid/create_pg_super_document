# test_re_compile

## Location
src/test/modules/test_regex/test_regex.c: 161 - 201

## Overview
test_re_compile is a static function that compiles a regular expression pattern using PostgreSQL's regex engine with proper character encoding handling and collation support.

## Definition
static void test_re_compile(text *text_re, int cflags, Oid collation, regex_t *result_re)

## Detailed Description
This function compiles a regular expression pattern from a PostgreSQL TEXT object into a compiled regex_t structure. It handles the conversion from the database encoding to wide characters (pg_wchar) which is required by Spencer's regex package used internally by PostgreSQL. The function properly manages memory allocation and provides comprehensive error handling for invalid patterns.

The compilation process includes:
1. Extracting the pattern string from the TEXT object
2. Converting from database encoding to wide characters
3. Calling the core regex compilation function with specified flags and collation
4. Cleaning up temporary memory and handling compilation errors

## Parameters / Member Variables
- : TEXT object containing the regular expression pattern in database encoding
- : Compilation flags that control regex behavior (e.g., case sensitivity, extended syntax)
- : OID of the collation to use for LC_CTYPE-dependent character classification
- : Output parameter where the compiled regex_t structure is stored

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY_EXHDR (gets size of TEXT object excluding header)
  - VARDATA_ANY (gets data pointer from TEXT object)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (converts multibyte string to wide characters)
  - pg_regcomp (core regex compilation function)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [pg_regerror](../p/pg_regerror.md) (gets error message for failed compilation)
  - ereport/ERROR (PostgreSQL error reporting)
- Called from (representative examples):
  - [test_regex](test_regex.md) (main regex testing function)

## Notes and Other Information
- This is a static (internal) function within the test_regex module
- Requires caller to eventually call pg_regfree() on the result to avoid memory leaks
- Handles character encoding conversion properly for international text
- Provides detailed error messages when pattern compilation fails
- Uses PostgreSQL's memory management functions for proper cleanup
- Located in src/test/modules/test_regex/test_regex.c:161-201