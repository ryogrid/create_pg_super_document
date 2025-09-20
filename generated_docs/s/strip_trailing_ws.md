# strip_trailing_ws

## Location
[src/pl/plperl/plperl.c:1061-1074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1061-L1074)

## Overview
Removes trailing whitespace characters from a string, specifically designed to clean up Perl error messages that often end with newlines.

## Definition

```c
static char *
strip_trailing_ws(const char *msg)
```
## Detailed Description
This utility function creates a copy of the input string and removes all trailing whitespace characters from it. The function is primarily used to clean up Perl error messages, which frequently include trailing newlines or other whitespace that would interfere with PostgreSQL's error reporting formatting.

The function works by:
1. Creating a duplicate of the input string using pstrdup()
2. Finding the length of the string
3. Iterating backwards from the end, removing whitespace characters
4. Null-terminating the string at the new length
5. Returning the cleaned string

## Parameters / Member Variables
- : The input string to clean (const char *) - the original string is not modified

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - strlen (standard C library function)
  - isspace (standard C library character classification function)
- Called from (representative examples):
  - [plperl_trusted_init](../p/plperl_trusted_init.md) (for cleaning PLC_TRUSTED and utf8fix error messages)
  - [plperl_untrusted_init](../p/plperl_untrusted_init.md) (for cleaning plperlu_init error messages)  
  - [plperl_create_sub](../p/plperl_create_sub.md) (for cleaning function compilation errors)
  - [plperl_call_perl_func](../p/plperl_call_perl_func.md) (for cleaning function execution errors)
  - [plperl_call_perl_trigger_func](../p/plperl_call_perl_trigger_func.md) (for cleaning trigger function errors)
  - [select_perl_context](select_perl_context.md) (for cleaning context setup errors)
  - PLPERL_RESTORE_LOCALE (for cleaning locale restoration errors)

## Notes and Other Information
- The function allocates new memory for the result string, so the caller is responsible for freeing it
- Uses PostgreSQL's pstrdup() rather than standard strdup() to integrate with PostgreSQL's memory management
- The isspace() function handles various whitespace characters (space, tab, newline, etc.)
- This function is essential for proper error message formatting in PL/Perl since Perl frequently adds trailing newlines to error messages
- The function safely handles empty strings and strings with no trailing whitespace