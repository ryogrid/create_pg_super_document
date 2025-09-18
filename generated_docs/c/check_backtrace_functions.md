# check_backtrace_functions

## Location
src/backend/utils/error/elog.c: 2164 - 2222

## Overview
check_backtrace_functions is a GUC check hook that validates and processes the backtrace_functions configuration parameter, converting a comma-separated list of function names into a null-terminated format.

## Definition


## Detailed Description
check_backtrace_functions validates and processes the backtrace_functions PostgreSQL configuration parameter. It parses a comma-separated string of function names and converts it into a special format for efficient scanning during error processing.

The function performs the following operations:
1. Validates that input contains only valid characters (C identifiers, commas, and whitespace)
2. Handles empty strings by setting extra to NULL
3. Allocates memory for the processed output
4. Converts the input by replacing commas with null characters and removing whitespace
5. Creates a double-null-terminated string for easy scanning

The output format allows efficient, reentrant scanning when errors occur without requiring strtok() or similar non-reentrant functions.

## Parameters / Member Variables
- : Pointer to the new configuration value string to be validated and processed
- : Pointer to store the processed output data (double-null-terminated string)
- : GucSource indicating the source of the configuration change

## Dependencies
- Functions called/Symbols referenced:
  - GucSource (type)
  - GUC_check_errdetail (for error reporting)
  - guc_malloc (for memory allocation)
  - strlen, strspn (C library functions)

- Called from (representative examples):
  - GUC_HOOKS_H (referenced in header file)

## Notes and Other Information
- Returns true if validation succeeds, false otherwise
- Uses strspn() to validate allowed characters: alphanumeric, underscore, comma, space, newline, tab
- The output format is a null-separated, double-null-terminated list of function names
- Whitespace characters (space, newline, tab) are ignored and removed from output
- Memory allocation is done via guc_malloc() with ERROR level
- Designed to avoid non-reentrant functions like strtok() for thread safety
- The processed string format enables efficient scanning during error backtrace generation
- Used as part of PostgreSQL's GUC (Grand Unified Configuration) system