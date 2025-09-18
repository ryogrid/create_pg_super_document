# regrolein

## Location
src/backend/utils/adt/regproc.c: 1541 - 1582

## Overview
Converts a role name string to its corresponding role OID, providing input parsing functionality for the regrole type.

## Definition
```c
Datum regrolein(PG_FUNCTION_ARGS)
```

## Detailed Description
The regrolein function serves as the input function for PostgreSQL's regrole type, which provides a symbolic way to reference database roles. This function takes a string representation of a role and converts it to the corresponding OID from the pg_authid system catalog.

The function handles multiple input formats:
- Role names: Looks up the role by name in pg_authid and returns its OID
- Numeric OIDs: Accepts numeric OID values directly for symmetry with output operations  
- Special dash notation: The string "-" represents an unknown/invalid role (OID 0)

The function includes comprehensive error handling for invalid names, non-existent roles, and bootstrap mode restrictions. It uses the standard PostgreSQL error reporting mechanisms and supports soft error handling through the error context parameter.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Input string (role name, numeric OID, or "-")
  - Error context for soft error handling
  - Return value storage

## Dependencies
- Functions called/Symbols referenced:
  - [parseDashOrOid](../p/parseDashOrOid.md) (handles dash notation and numeric OID parsing)
  - PG_RETURN_OID (returns OID values)
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - [stringToQualifiedNameList](../s/stringToQualifiedNameList.md) (parses input string to name list)
  - ereturn (error return with context support)
  - get_role_oid (looks up role OID by name)
- Called from (representative examples):
  - [to_regrole](../t/to_regrole.md) (conversion function)

## Notes and Other Information
- Located in src/backend/utils/adt/regproc.c:1541-1582
- Part of the regrole type input/output function suite
- Restricted in bootstrap mode - only accepts numeric OIDs during system initialization
- Validates that role names contain exactly one component (no schema qualification)
- Returns NULL for parsing failures when using error context
- The regrole type enables referencing roles symbolically while storing them as OIDs
- Provides symmetry with regroleout for complete type I/O operations