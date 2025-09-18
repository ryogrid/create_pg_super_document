# PG_LOG_OFF

## Location
[src/include/common/logging.h:48-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/logging.h#L48-L60)

## Overview
PG_LOG_OFF is an enum value in the pg_log_level enumeration that represents a special log level used to turn off all logging functionality in PostgreSQL frontend programs.

## Definition


## Detailed Description
PG_LOG_OFF serves as a special sentinel value within the pg_log_level enumeration to indicate that all logging should be disabled. It is explicitly not intended to be used as an actual message log level for individual log messages, but rather as a configuration value to completely suppress logging output. This enum value is part of the logging framework designed for PostgreSQL frontend programs.

The enum value is positioned as the highest value in the pg_log_level enumeration, making it useful for comparisons when determining whether logging should be active. When __pg_log_level is set to PG_LOG_OFF, it effectively disables all log message output regardless of the individual message's log level.

## Parameters / Member Variables
This is an enum constant, so it has no parameters or member variables.

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_level (parent enum)
- Called from (representative examples):
  - No direct references found (used as configuration value)

## Notes and Other Information
- Located in src/include/common/logging.h:48
- Part of the logging framework for PostgreSQL frontend programs
- Not intended for use as an actual message log level
- Used for configuration to disable all logging
- Highest value in the pg_log_level enumeration hierarchy
- Works in conjunction with __pg_log_level global variable for log level filtering