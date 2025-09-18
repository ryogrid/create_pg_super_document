# pg_logging_config

## Location
src/common/logging.c: 163 - 172

## Overview
A simple function that updates the global logging configuration flags to control the behavior and formatting of log output.

## Definition
void pg_logging_config(int new_flags)

## Detailed Description
This function provides a straightforward way to modify the logging system's behavior by updating the global log_flags variable. The primary use case is to control output formatting, particularly for regression testing where specific output formats are required.

Currently, the only defined flag is PG_LOG_FLAG_TERSE, which modifies the logging output to produce a more concise format that matches the exact requirements of PostgreSQL's regression test suite. This flag affects how messages are formatted and displayed.

The function simply assigns the new flags value to the static log_flags variable, which is then referenced by other parts of the logging system to determine formatting behavior.

## Parameters / Member Variables
- : Integer value representing the logging flags to be set. Currently supports PG_LOG_FLAG_TERSE (value 1) for terse output formatting required by regression tests.

## Dependencies
- Functions called/Symbols referenced:
  - None (simple assignment operation)
- Called from (representative examples):
  - process_file (in psql command processing)
  - Various startup functions in psql

## Notes and Other Information
- This is a simple setter function with no validation or error checking
- The log_flags variable is static to the logging.c file and not directly accessible outside the module
- PG_LOG_FLAG_TERSE is currently the only defined flag, used primarily for regression testing
- The function can be called multiple times to change logging behavior during program execution
- No memory allocation or deallocation is involved in this operation