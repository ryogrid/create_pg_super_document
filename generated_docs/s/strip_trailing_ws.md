# strip_trailing_ws

## Location
src/pl/plperl/plperl.c: 1061 - 1074

## Overview
Removes trailing whitespace characters from a string, specifically designed to clean up Perl error messages that often end with newlines.

## Definition


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
  - pstrdup (PostgreSQL string duplication function)
  - strlen (standard C library function)
  - isspace (standard C library character classification function)
- Called from (representative examples):
  - plperl_trusted_init (for cleaning PLC_TRUSTED and utf8fix error messages)
  - plperl_untrusted_init (for cleaning plperlu_init error messages)  
  - plperl_create_sub (for cleaning function compilation errors)
  - plperl_call_perl_func (for cleaning function execution errors)
  - plperl_call_perl_trigger_func (for cleaning trigger function errors)
  - select_perl_context (for cleaning context setup errors)
  - PLPERL_RESTORE_LOCALE (for cleaning locale restoration errors)

## Notes and Other Information
- The function allocates new memory for the result string, so the caller is responsible for freeing it
- Uses PostgreSQL's pstrdup() rather than standard strdup() to integrate with PostgreSQL's memory management
- The isspace() function handles various whitespace characters (space, tab, newline, etc.)
- This function is essential for proper error message formatting in PL/Perl since Perl frequently adds trailing newlines to error messages
- The function safely handles empty strings and strings with no trailing whitespace