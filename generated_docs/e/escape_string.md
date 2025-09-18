# escape_string

## Location
src/bin/psql/tab-complete.c: 5930 - 5952

## Overview
Escapes a string for safe use as a PostgreSQL string literal by applying proper SQL escaping rules.

## Definition
static char *escape_string(const char *text)

## Detailed Description
This function creates a properly escaped version of the input text suitable for use as a string literal in PostgreSQL queries. It uses PQescapeStringConn() to perform the escaping, which handles special characters according to PostgreSQL's string literal rules. The function allocates sufficient memory (up to twice the original length plus null terminator) to accommodate potential escape sequences.

This is essential for preventing SQL injection and ensuring that user input containing special characters like quotes, backslashes, and other SQL metacharacters is properly handled when constructing queries.

## Parameters / Member Variables
- text: The input string to be escaped

## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - pg_malloc
  - [PQescapeStringConn](../P/PQescapeStringConn.md)
- Called from (representative examples):
  - [_complete_from_query](../c/_complete_from_query.md) (multiple locations)
  - [make_like_pattern](../m/make_like_pattern.md)
  - [get_guctype](../g/get_guctype.md)
  - [escape_fmt_id](escape_fmt_id.md) (in test modules)

## Notes and Other Information
The returned string must be freed by the caller. The function allocates memory for the worst-case scenario where every character might need escaping. Uses the current database connection (pset.db) for context-aware escaping that considers the connection's encoding and other settings.