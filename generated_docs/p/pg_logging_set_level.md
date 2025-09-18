# pg_logging_set_level

## Location
src/common/logging.c: 173 - 181

## Overview
Sets the minimum log level threshold for message output, determining which log messages will be displayed based on their severity level.

## Definition
void pg_logging_set_level(enum pg_log_level new_level)

## Detailed Description
This function configures the global logging threshold by updating the __pg_log_level variable. Only log messages at or above the specified level will be displayed, while messages below the threshold will be suppressed.

The function is typically used immediately after pg_logging_init() by programs that want to override the default INFO log level. For example, a program might set the level to DEBUG for verbose output, or to WARNING to reduce verbosity.

The logging system uses this threshold in conjunction with the pg_log_debug macros, which check the current log level before generating debug output to avoid performance overhead when debug logging is disabled.

## Parameters / Member Variables
- : An enum pg_log_level value that specifies the minimum severity level for displayed messages. Valid values include:
  - PG_LOG_DEBUG: Show all messages including debug information
  - PG_LOG_INFO: Show informational messages and above (default)
  - PG_LOG_WARNING: Show only warnings and errors
  - PG_LOG_ERROR: Show only error messages
  - PG_LOG_OFF: Suppress all logging output

## Dependencies
- Functions called/Symbols referenced:
  - enum pg_log_level (parameter type)
- Called from (representative examples):
  - [main](../m/main.md) functions in pg_dump, pg_restore, pg_dumpall, pg_createsubscriber

## Notes and Other Information
- Should typically be called immediately after pg_logging_init() if a non-default log level is desired
- The default log level set by pg_logging_init() is PG_LOG_INFO
- Setting the level affects all subsequent logging calls throughout the program
- Debug messages use conditional compilation to avoid overhead when debug logging is disabled
- The function performs a simple assignment with no validation of the input parameter
- Changes take effect immediately for all subsequent log messages