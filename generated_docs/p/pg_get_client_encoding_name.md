# pg_get_client_encoding_name

## Location
src/backend/utils/mb/mbutils.c: 345 - 355

## Overview
Returns the string name of the currently active client encoding.

## Definition
```c
const char *pg_get_client_encoding_name(void)
```

## Detailed Description
pg_get_client_encoding_name is a simple accessor function that returns the human-readable string name of the current client encoding. It accesses the global ClientEncoding variable, which points to an entry in the pg_enc2name_tbl array, and returns the name field from that entry.

This function provides a clean interface for obtaining the encoding name as a string (e.g., "UTF8", "LATIN1", "EUC_JP") rather than the numeric encoding ID. The returned string is a constant and should not be modified by the caller.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ClientEncoding (global variable - accesses name field)
- Called from:
  - No direct references found in the codebase (may be used through function pointers or external interfaces)

## Notes and Other Information
- Returns a const char* pointing to the encoding name string
- Provides the complement to pg_get_client_encoding(), returning the name instead of the numeric ID
- The returned string is constant and should not be freed or modified
- Relies on the global ClientEncoding variable being properly initialized
- Common encoding names include "UTF8", "LATIN1", "EUC_JP", "WIN1252", etc.
- May be exposed through SQL functions or used in logging and error reporting