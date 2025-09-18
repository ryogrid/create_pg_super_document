# pg_get_function_arguments

## Location
src/backend/utils/adt/ruleutils.c: 3133 - 3158

## Overview
Returns a nicely-formatted list of arguments for a function, representing everything that would appear between the parentheses in a CREATE FUNCTION statement.

## Definition


## Detailed Description
This SQL-callable function takes a function OID as input and returns a formatted text representation of the function's argument list. It retrieves the function's metadata from the system catalog (pg_proc) and formats the arguments in a human-readable form suitable for display in CREATE FUNCTION statements or documentation. The function handles the complete argument specification including parameter names, types, and modes.

## Parameters / Member Variables
- : OID of the target function whose arguments are to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - print_function_arguments
  - string_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Returns NULL if the function OID is invalid or the function doesn't exist
- Uses the print_function_arguments helper function with parameters (false, true) to generate the formatted output
- Part of the ruleutils.c module which provides functions for formatting SQL statements and database object definitions
- The function is exposed as a SQL function for use in queries and system views