# SetDatabaseEncoding

## Location
src/backend/utils/mb/mbutils.c: 1161 - 1170

## Overview
Sets the global database encoding by validating the provided encoding identifier and updating the global DatabaseEncoding pointer.

## Definition
```c
void SetDatabaseEncoding(int encoding)
```

## Detailed Description
This function is responsible for establishing the database's character encoding during database initialization. It validates that the provided encoding identifier is a valid backend encoding using the PG_VALID_BE_ENCODING macro, then sets the global DatabaseEncoding pointer to point to the appropriate entry in the pg_enc2name_tbl array. This global setting affects how the database handles character encoding throughout the session.

The function includes an assertion to ensure consistency between the provided encoding identifier and the encoding stored in the selected table entry, providing additional safety in debug builds.

## Parameters / Member Variables
- `encoding`: The numeric identifier for the character encoding to set as the database encoding

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_BE_ENCODING (macro for validation)
  - elog (for error reporting)
  - Assert (debug assertion macro)
  - pg_enc2name_tbl (global encoding table)
  - DatabaseEncoding (global variable being set)
- Called from (representative examples):
  - [CheckMyDatabase](../C/CheckMyDatabase.md)

## Notes and Other Information
- This function is typically called during database startup/initialization
- The function will terminate the process with an ERROR if an invalid encoding is provided
- The global DatabaseEncoding pointer is used throughout PostgreSQL for encoding-related operations
- The function assumes the pg_enc2name_tbl array is properly initialized
- Only valid backend encodings are accepted; client-only encodings are rejected