# is_an_int

## Location
[src/bin/pgbench/pgbench.c:951-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L951-L987)

## Overview
A static utility function that validates whether a given string represents a valid integer according to a specific regular expression pattern.

## Definition
static bool is_an_int(const char *str)

## Detailed Description
is_an_int performs string validation to determine if the input matches the pattern "^\\s*[-+]?[0-9]+$", which represents an integer with optional leading whitespace and an optional sign. The function systematically parses the string by first skipping any leading whitespace characters, then checking for an optional plus or minus sign, ensuring at least one digit is present, consuming all consecutive digits, and finally verifying that the end of string is reached. This validation is consistent with PostgreSQL's strtoint64 function behavior and is used in pgbench for parsing integer values from configuration or command-line inputs.

## Parameters / Member Variables
- str: Pointer to the null-terminated string to be validated as an integer

## Dependencies
- Functions called/Symbols referenced:
  - isspace (standard C library function for whitespace detection)
  - isdigit (standard C library function for digit detection)
  - Uses unsigned char casting for proper character classification

- Called from (representative examples):
  - [makeVariableValue](../m/makeVariableValue.md)

## Notes and Other Information
- Implements validation logic consistent with PostgreSQL's strtoint64 function
- Handles optional leading whitespace and optional plus/minus signs
- Requires at least one digit to be present for a valid integer
- Uses proper unsigned char casting to avoid undefined behavior with character classification functions
- Returns false for empty strings, strings with only whitespace, or strings containing non-digit characters after the optional sign
- Essential for input validation in pgbench's variable and configuration parsing