# ParseVariableBool

## Location
[src/bin/psql/variables.c:107-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L107-L155)

## Overview
Parses a string value as a boolean and stores the result, supporting various common boolean representations with case-insensitive partial matching.

## Definition

```c
bool
ParseVariableBool(const char *value, const char *name, bool *result)
```
## Detailed Description
This function attempts to interpret a string value as a boolean and stores the parsed result in the provided output parameter. It supports a comprehensive set of boolean representations including "true", "false", "yes", "no", "on", "off", "1", and "0". The function uses case-insensitive comparison and allows unique prefixes for most values (e.g., "t" for "true", "f" for "false").

Special handling is implemented for "on" and "off" values where a minimum of 2 characters is required for matching to avoid ambiguity with single character "o". The function treats NULL input as an empty string, which results in a parsing error. When parsing fails, the original value in the result parameter is preserved, and an error message is optionally logged.

## Parameters / Member Variables
- : The string value to parse as a boolean. NULL is treated as an empty string
- : The name of the variable being assigned (used for error reporting). Pass NULL to suppress error messages
- : Pointer to bool where the parsed result will be stored. Only modified on successful parsing

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (PostgreSQL case-insensitive string comparison)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (PostgreSQL case-insensitive string comparison)
  - pg_log_error (PostgreSQL error logging function)
  - strlen (standard C library function)
- Called from (representative examples):
  - [exec_command_connect](../e/exec_command_connect.md)
  - [exec_command_timing](../e/exec_command_timing.md)
  - [is_true_boolean_expression](../i/is_true_boolean_expression.md)
  - Various hook functions (autocommit_hook, on_error_stop_hook, etc.)

## Notes and Other Information
- Supports partial matching for unique prefixes ("t" matches "true", "f" matches "false")
- Requires at least 2 characters for "on"/"off" to avoid ambiguity with "o"
- Returns true for successful parsing, false for invalid input
- Does not modify *result when parsing fails, preserving the original value
- Used extensively throughout psql for parsing boolean configuration variables and command options
- Case-insensitive matching allows flexible user input ("TRUE", "True", "true" all work)