# function_parse_error_transpose

## Location
[src/backend/catalog/pg_proc.c:1002-1068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_proc.c#L1002-L1068)

## Overview
Adjusts syntax error positions occurring inside function bodies of CREATE FUNCTION or DO commands to reference the original command text or set up internal query error reporting.

## Definition
bool function_parse_error_transpose(const char *prosrc)

## Detailed Description
This function handles syntax error position adjustment for function validators and anonymous-block handlers. When a syntax error occurs within a function body, the error position is initially relative to the function body string. This function attempts to transpose that position to reference the original CREATE FUNCTION or DO command text. If successful, it updates the error position to point to the correct location in the original query. If unsuccessful (e.g., when the function source cannot be located in the original text), it converts the error to an "internal query" error with the function source as the internal query text.

The function works by leveraging the ActivePortal to access the original query text and then using pattern matching to locate the function source within that text.

## Parameters / Member Variables
- `prosrc`: The function source code string that contains the syntax error

## Dependencies
- Functions called/Symbols referenced:
  - [geterrposition](../g/geterrposition.md)
  - [getinternalerrposition](../g/getinternalerrposition.md)  
  - PORTAL_ACTIVE
  - [match_prosrc_to_query](../m/match_prosrc_to_query.md)
  - [errposition](../e/errposition.md)
  - [internalerrposition](../i/internalerrposition.md)
  - [internalerrquery](../i/internalerrquery.md)
- Called from (representative examples):
  - [sql_function_parse_error_callback](../s/sql_function_parse_error_callback.md)

## Notes and Other Information
- Returns true if a syntax error was processed, false if not
- Only processes errors that have cursor positions (either regular or internal)
- Relies on ActivePortal being available and active to access original query text
- Falls back to internal query error reporting when original text matching fails
- Used primarily for SQL-language functions but can be used by any function validator
- The function implements a "hack" by accessing the original query text from ActivePortal
- Quietly gives up in unusual situations like logical replication workers where ActivePortal may not be available