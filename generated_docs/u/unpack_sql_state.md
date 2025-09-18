# unpack_sql_state

## Location
src/backend/utils/error/elog.c: 3166 - 3185

## Overview
Converts a packed integer SQL state code back into its 5-character string representation for error reporting and logging.

## Definition
```c
char *unpack_sql_state(int sql_state)
```

## Detailed Description
The `unpack_sql_state` function reverses the process of MAKE_SQLSTATE by extracting a 5-character SQL state string from a packed integer representation. Each character is stored as 6 bits in the integer, allowing all 5 SQL state characters to fit within a 32-bit integer. The function uses the PGUNSIXBIT macro to extract each 6-bit field and convert it back to its ASCII character representation.

SQL state codes are standardized 5-character identifiers defined by the SQL standard to categorize different types of errors and warnings. PostgreSQL uses these codes for consistent error reporting across different interfaces and logging destinations.

## Parameters / Member Variables
- `sql_state`: Packed integer representation of the SQL state code (created by MAKE_SQLSTATE)

## Dependencies
- Functions called/Symbols referenced:
  - PGUNSIXBIT (macro for extracting 6-bit fields)
- Called from (representative examples):
  - log_status_format
  - send_message_to_server_log
  - send_message_to_frontend
  - write_csvlog
  - write_jsonlog
  - pg_input_error_info
  - Various PL language modules (plpython, pltcl)

## Notes and Other Information
- Returns a pointer to a static buffer, so the result is only valid until the next call
- The static buffer is 12 characters long (though only 6 are used: 5 for the state plus null terminator)
- Used extensively throughout PostgreSQL's error handling and logging systems
- Critical for maintaining SQL standard compliance in error reporting
- Thread-safety consideration: uses static buffer, so concurrent calls will overwrite each other