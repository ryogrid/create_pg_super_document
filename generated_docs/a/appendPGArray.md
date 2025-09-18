# appendPGArray

## Location
src/fe_utils/string_utils.c: 902 - 965

## Overview
Appends one element to the text representation of a 1-dimensional PostgreSQL array with proper formatting and quoting.

## Definition


## Detailed Description
This function adds a single element to a PostgreSQL array literal being constructed in a PQExpBuffer. It handles all formatting details including comma insertion, value quoting, and escape sequence generation to match PostgreSQL's array_out() behavior.

The function determines whether quoting is needed based on:
- Empty strings (always quoted)
- Literal "NULL" values (always quoted) 
- Presence of special characters: quotes, backslashes, braces, commas, whitespace

When quoting is required, it properly escapes internal quotes and backslashes. The caller is responsible for providing the opening '{' and closing '}' of the array.

## Parameters / Member Variables
- : PQExpBuffer to append the formatted array element to
- : String value to add as an array element

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar
  - appendPQExpBufferStr
  - pg_strcasecmp

- Called from (representative examples):
  - getNamespaces (src/bin/pg_dump/pg_dump.c:5725, 5729)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:902-965
- Assumes type delimiter is comma (',')
- Automatically adds commas between elements (detects if buffer ends with '{')
- Quoting logic matches PostgreSQL's array_out() function behavior
- Handles whitespace characters matching scanner_isspace() definition
- Used primarily by pg_dump utilities for constructing array literals in SQL output
- Part of the frontend utilities string manipulation library