# pg_collation_actual_version

## Location
src/backend/commands/collationcmds.c: 511 - 582

## Overview
pg_collation_actual_version is a SQL-callable function that returns the actual version string from the underlying collation provider for a given collation OID.

## Definition


## Detailed Description
This function implements the pg_collation_actual_version() SQL function, which queries the underlying collation library to obtain the current version information for a collation. The function handles two cases:
1. For the default collation (DEFAULT_COLLATION_OID), it retrieves locale information from pg_database
2. For regular collations, it retrieves information from pg_collation

The function determines the appropriate locale string based on the collation provider (libc uses collcollate/datcollate, others use colllocale/datlocale) and then calls get_collation_actual_version() to obtain the version from the provider library.

## Parameters / Member Variables
- Function takes one OID argument (collation OID) via PG_GETARG_OID(0)
- Returns version string as TEXT or NULL if no version is available

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - TextDatumGetCString
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - cstring_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - SQL queries using pg_collation_actual_version() function

## Notes and Other Information
- Exposed as a SQL function for administrative and diagnostic purposes
- Handles both default collation (from database settings) and regular collations differently
- Provider-aware: uses different catalog columns based on whether the provider is COLLPROVIDER_LIBC or another type
- Returns NULL when the underlying provider doesn't support version information
- Critical for detecting collation version mismatches that could indicate index corruption risks