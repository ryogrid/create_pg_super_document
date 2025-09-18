# make_like_pattern

## Location
src/bin/psql/tab-complete.c: 5953 - 5998

## Overview
Converts a string into a PostgreSQL LIKE pattern by escaping special characters and appending a wildcard for prefix matching.

## Definition
static char *make_like_pattern(const char *word)

## Detailed Description
This function transforms user input into a properly formatted LIKE pattern for PostgreSQL queries. It escapes the special LIKE metacharacters underscore (_) and percent (%) with backslashes, appends a percent sign to create a prefix match pattern, and then passes the result through escape_string() to make it safe for SQL query insertion.

The function also handles multibyte characters correctly by detecting high-bit-set characters and preserving them without modification, using PQmblenBounded() to determine character boundaries. This ensures proper handling of non-ASCII text in various client encodings.

## Parameters / Member Variables
- word: The input string to convert into a LIKE pattern

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - strlen
  - IS_HIGHBIT_SET
  - [PQmblenBounded](../P/PQmblenBounded.md)
  - [escape_string](../e/escape_string.md)
  - free
- Called from (representative examples):
  - [_complete_from_query](../c/_complete_from_query.md)
  - THING_NO_SHOW completion generator

## Notes and Other Information
The function creates a pattern that matches any string starting with the input word followed by any characters (prefix matching). Multibyte character handling prevents corruption in unsafe client encodings. The returned string is ready for direct insertion into SQL queries and must be freed by the caller. The intermediate buffer is automatically freed before returning.