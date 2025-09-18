# parse_max_rate

## Location
src/bin/pg_basebackup/pg_basebackup.c: 901 - 985

## Overview
A utility function that parses and validates a string representation of a transfer rate value, converting it to an integer value in kilobytes per second for use with the --max-rate option in pg_basebackup.

## Definition
```c
static int32 parse_max_rate(char *src)
```

## Detailed Description
This function performs comprehensive parsing and validation of transfer rate strings provided via the --max-rate command-line option. It accepts numeric values with optional unit suffixes (k for kilobytes, M for megabytes) and performs extensive validation including:

- Parsing the numeric component using strtod()
- Validating that the value is positive and within acceptable ranges
- Handling unit suffixes (k, M) with appropriate conversion
- Ensuring the final value fits within a 32-bit signed integer range
- Checking against predefined minimum and maximum rate limits

The function defaults to kilobytes when no suffix is specified, and megabyte values are converted to kilobytes by multiplying by 1024.

## Parameters / Member Variables
- `src`: A null-terminated string containing the transfer rate value to parse, potentially including a unit suffix (k or M)

## Dependencies
- Functions called/Symbols referenced:
  - strtod (standard C library function for string to double conversion)
  - isspace (standard C library function for whitespace checking)
  - pg_fatal (PostgreSQL error reporting function)
  - MAX_RATE_LOWER (minimum acceptable transfer rate constant)
  - MAX_RATE_UPPER (maximum acceptable transfer rate constant)
- Called from (representative examples):
  - main (in pg_basebackup.c for processing command-line arguments)

## Notes and Other Information
- The function is static, having internal linkage within the pg_basebackup.c file
- Default unit is kilobytes when no suffix is provided
- Supports two unit suffixes: "k" (kilobytes) and "M" (megabytes, converted to kilobytes)
- Performs both client-side and server-side validation to avoid unnecessary server connections for invalid values
- Returns an int32 value representing the rate in kilobytes per second
- Uses comprehensive error handling with descriptive error messages via pg_fatal()
- Handles floating-point input but converts to integer output
- Validates that the final result fits within the expected integer range