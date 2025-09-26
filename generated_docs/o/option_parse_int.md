# option_parse_int

## Location
[src/fe_utils/option_utils.c:50-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/option_utils.c#L50-L89)

## Overview
Parses and validates integer values from command-line option arguments with range checking.

## Definition

```c
bool
option_parse_int(const char *optarg, const char *optname,
				 int min_range, int max_range,
				 int *result)
```
## Detailed Description
This utility function provides robust parsing of integer command-line option values with comprehensive validation. It converts string arguments to integers while performing range validation and error handling. The function handles trailing whitespace gracefully and provides informative error messages for invalid input.

The parsing process includes:
1. Converting the string to integer using strtoint()
2. Skipping trailing whitespace
3. Validating that no non-whitespace characters remain
4. Checking that the value falls within the specified range
5. Storing the result if parsing succeeds

On success, the function returns true and optionally stores the parsed value. On failure, it logs an appropriate error message and returns false.

## Parameters / Member Variables
- : String containing the option argument to parse
- : Name of the option (used in error messages)
- : Minimum allowed value (inclusive)
- : Maximum allowed value (inclusive)
- : Optional pointer to store the parsed integer value (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [strtoint](../s/strtoint.md)
  - isspace
  - pg_log_error
  - errno (global variable)
  - ERANGE (macro)
- Called from (representative examples):
  - [main](../m/main.md) (in initdb)
  - [main](../m/main.md) (in pg_amcheck)
  - [main](../m/main.md) (in pg_basebackup)
  - [main](../m/main.md) (in pg_dump)
  - [main](../m/main.md) (in pgbench)
  - [main](../m/main.md) (in various other client utilities)

## Notes and Other Information
- Returns true on successful parsing, false on error
- The result parameter can be NULL if only validation is needed
- Automatically handles trailing whitespace in input
- Uses strtoint() for conversion, which is PostgreSQL's safe integer parsing function
- Provides detailed error messages that include the option name and acceptable range
- Part of the fe_utils library for consistent option parsing across PostgreSQL tools
- Essential for validating numeric parameters in command-line utilities