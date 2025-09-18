# appendStringInfoStringQuoted

## Location
src/backend/utils/mb/stringinfo_mb.c: 34 - 86

## Overview
Appends a string to a StringInfo buffer with single quotes around it, doubling any existing single quotes for proper SQL escaping, and optionally truncating to a maximum length with ellipsis.

## Definition
void appendStringInfoStringQuoted(StringInfo str, const char *s, int maxlen)

## Detailed Description
This function provides safe string concatenation with SQL-style quoting for PostgreSQL's StringInfo data structure. It takes an input string and appends it to the target StringInfo buffer, wrapping it in single quotes and escaping any existing single quotes by doubling them (e.g., "John's" becomes "'John''s'"). 

The function supports length limitation through the maxlen parameter. When maxlen is non-negative and less than the input string length, the function uses multibyte-aware clipping (pg_mbcliplen) to ensure proper character boundary handling, then appends an ellipsis ("...") to indicate truncation. This makes it particularly useful for logging and error reporting where string length needs to be controlled while maintaining readability.

The implementation efficiently processes the string by searching for single quote characters and copying chunks between quotes, doubling each quote as it encounters them. This approach minimizes memory allocations and string operations.

## Parameters / Member Variables
- : Target StringInfo buffer where the quoted string will be appended
- : Source string to be quoted and appended (null-terminated C string)
- : Maximum number of bytes to copy from the source string; if negative, the entire string is used

## Dependencies
- Functions called/Symbols referenced:
  - pg_mbcliplen (for multibyte-aware string clipping)
  - pnstrdup (for creating a truncated copy of the input string)
  - appendStringInfoCharMacro (for appending the opening single quote)
  - appendBinaryStringInfoNT (for appending string chunks including quotes)
  - appendStringInfo (for appending the final chunk with closing quote and optional ellipsis)
  - strlen (for getting string length)
  - strchr (for finding single quote characters)
  - pfree (for freeing temporary memory)
- Called from (representative examples):
  - BuildParamLogString (in src/backend/nodes/params.c)
  - bind_param_error_callback (in src/backend/tcop/postgres.c)

## Notes and Other Information
- This function is specifically designed for PostgreSQL's multibyte character support, using pg_mbcliplen to ensure character boundaries are respected when truncating
- The function is particularly useful for safely formatting SQL strings in log messages and error reports
- Memory management is handled carefully - temporary copies are freed after use
- The ellipsis indicator ("...") is only added when actual truncation occurs
- Single quotes are escaped by doubling them, following SQL standard conventions
- The function is declared in src/include/mb/stringinfo_mb.h as part of PostgreSQL's multibyte string utilities