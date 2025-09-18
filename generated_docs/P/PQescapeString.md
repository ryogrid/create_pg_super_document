# PQescapeString

## Location
src/interfaces/libpq/fe-exec.c: 4199 - 4213

## Overview
PQescapeString is a public libpq function that escapes arbitrary strings for use in SQL queries using static default encoding and string settings when no database connection is available.

## Definition


## Detailed Description
PQescapeString provides string escaping functionality for situations where a database connection is not available. This function uses static default values for client encoding and standard string settings rather than connection-specific settings.

The function is simpler to use than PQescapeStringConn since it doesn't require a connection handle, but it's less safe because it cannot account for connection-specific encoding or server settings. It uses static global variables (static_client_encoding and static_std_strings) that represent default or last-known settings.

This function is essentially a wrapper around PQescapeStringInternal with NULL connection and error parameters, relying on static configuration values.

## Parameters / Member Variables
- : Output buffer where the escaped string will be written (must be at least 2*length + 1 bytes)
- : Input string to be escaped
- : Maximum length of the source string to process

## Dependencies
- Functions called/Symbols referenced:
  - PQescapeStringInternal
  - static_client_encoding (global variable)
  - static_std_strings (global variable)
- Called from (representative examples):
  - quote_postgres
  - escape_string (in test modules)

## Notes and Other Information
- This function is less safe than PQescapeStringConn because it uses static default settings
- No error reporting is available since no error parameter is provided
- Uses static global variables for encoding and string standard settings
- Recommended to use PQescapeStringConn when a connection is available
- The output buffer must be at least 2*length + 1 bytes to accommodate worst-case escaping
- Always produces a NUL-terminated output string
- Cannot provide connection-specific error reporting or encoding validation
- Mainly provided for backward compatibility and simple use cases where connection information is not available