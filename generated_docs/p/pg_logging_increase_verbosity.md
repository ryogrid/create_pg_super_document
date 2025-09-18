# pg_logging_increase_verbosity

## Location
src/common/logging.c: 182 - 192

## Overview
Increases the verbosity of logging output by decreasing the minimum log level threshold, typically invoked by command-line switches like --verbose.

## Definition
void pg_logging_increase_verbosity(void)

## Detailed Description
This function increases logging verbosity by moving the current log level one step toward more detailed output. The PostgreSQL logging system uses an enum where lower numeric values represent more verbose logging levels, so increasing verbosity actually means decreasing the __pg_log_level value.

The function implements a safety check to prevent the log level from being decreased below a minimum threshold (PG_LOG_NOTSET + 1). This prevents invalid log levels and ensures the logging system remains in a valid state.

Typical usage involves command-line argument parsing where each occurrence of a verbose flag (like -v or --verbose) calls this function to incrementally increase the verbosity. Multiple calls will progressively make the logging more verbose, moving from ERROR to WARNING to INFO to DEBUG levels.

The progression of verbosity levels from least to most verbose is:
- PG_LOG_ERROR (errors only)
- PG_LOG_WARNING (warnings and errors)  
- PG_LOG_INFO (info, warnings, and errors)
- PG_LOG_DEBUG (all messages including debug information)

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - PG_LOG_NOTSET (enum constant used as boundary check)
- Called from (representative examples):
  - [main](../m/main.md) functions in various PostgreSQL utilities (pg_dump, pg_restore, pgbench, pg_amcheck, etc.)

## Notes and Other Information
- Designed to be called multiple times for incremental verbosity increases
- Each call moves one level toward more verbose output
- Includes bounds checking to prevent invalid log level values  
- Commonly used in command-line argument processing for --verbose flags
- The enum design (lower values = more verbose) is counterintuitive but allows this simple decrement operation
- Will not decrease the log level below PG_LOG_DEBUG level due to the boundary check
- Changes take effect immediately for all subsequent logging calls