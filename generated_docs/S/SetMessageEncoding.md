# SetMessageEncoding

## Location
src/backend/utils/mb/mbutils.c: 1171 - 1186

## Overview
Sets the global message encoding by validating the provided encoding identifier and updating the global MessageEncoding pointer for error messages and client communication.

## Definition
```c
void SetMessageEncoding(int encoding)
```

## Detailed Description
This function establishes the character encoding used for error messages and client communication. Unlike SetDatabaseEncoding, this function uses only an assertion for validation rather than an elog() call, as indicated by the comment that some calls happen before the error logging system is ready. It sets the global MessageEncoding pointer to the appropriate entry in the pg_enc2name_tbl array.

The message encoding affects how PostgreSQL formats and sends messages to clients, ensuring that error messages, notices, and other communication are properly encoded for the client's expected character set.

## Parameters / Member Variables
- `encoding`: The numeric identifier for the character encoding to set as the message encoding

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro for validation)
  - Assert (debug assertion macro)
  - pg_enc2name_tbl (global encoding table)
  - MessageEncoding (global variable being set)
- Called from (representative examples):
  - [pg_perm_setlocale](../p/pg_perm_setlocale.md) (multiple calls)

## Notes and Other Information
- This function is called during early initialization when elog() may not be available yet
- Uses assertions rather than error logging for validation due to initialization timing constraints
- The global MessageEncoding pointer affects client communication encoding throughout the session
- Unlike SetDatabaseEncoding, this accepts any valid encoding (not just backend encodings)
- Critical for ensuring proper character encoding in error messages and client responses
- Called during locale setup to ensure message encoding matches the locale settings