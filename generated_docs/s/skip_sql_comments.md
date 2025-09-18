# skip_sql_comments

## Location
[src/bin/pgbench/pgbench.c:5550-5584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5550-L5584)

## Overview
Skips over leading whitespace and SQL-style line comments (--) to find the start of actual SQL command content.

## Definition


## Detailed Description
The skip_sql_comments function processes SQL command text to locate the beginning of executable SQL content by skipping over non-essential elements. It iteratively advances through the input string, ignoring whitespace characters and SQL line comments that begin with '--'. The function handles comments by finding the newline character that terminates them, then continues processing from the next line. This preprocessing is essential for pgbench's SQL command parsing pipeline, ensuring that only meaningful SQL content is processed.

The function uses a simple state machine approach, continuously checking each character and advancing the pointer until it encounters the first non-whitespace, non-comment character. If the entire string contains only whitespace and comments, the function returns NULL to indicate no executable content was found.

## Parameters / Member Variables
- : Pointer to a null-terminated string containing the SQL command text to process

## Dependencies
- Functions called/Symbols referenced:
  - isspace: Standard C library function to check for whitespace characters
  - strncmp: Standard C library function to compare string prefixes
  - strchr: Standard C library function to find character occurrences
- Called from (representative examples):
  - [create_sql_command](../c/create_sql_command.md): Main function that processes SQL commands in pgbench scripts

## Notes and Other Information
- The function modifies the input pointer but does not alter the actual string content
- Returns NULL if no executable SQL content is found after skipping comments and whitespace
- Only handles '--' style line comments, not /* */ block comments
- The function assumes properly formed input and does not perform comprehensive SQL syntax validation
- Used as a preprocessing step before more detailed SQL command parsing and validation
- The comment handling stops at newline characters, following standard SQL comment behavior
- Whitespace detection uses the standard isspace() function, which handles various whitespace characters