# regclassin

## Location
src/backend/utils/adt/regproc.c: 882 - 924

## Overview
Converts a class name (table, view, sequence, etc.) to its corresponding OID, serving as the input function for the regclass data type.

## Definition


## Detailed Description
The  function is an input function for the  data type in PostgreSQL. It accepts either a class name (potentially schema-qualified) or a numeric OID and converts it to the appropriate relation OID. The function handles several input formats:

1. **Dash ("-")**: Represents an unknown or invalid OID (returns 0)
2. **Numeric OID**: Directly parsed and returned for symmetry with output routines
3. **Class name**: Looked up in the system catalogs, potentially schema-qualified

The function performs name resolution using the current search path and validates that the relation exists. It handles both simple names like "mytable" and schema-qualified names like "public.mytable". The function includes special handling for bootstrap mode where only numeric OIDs are accepted.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - Argument 0:  - The class name or OID string to convert
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract string argument from function call
  -  - Handle dash or numeric OID parsing  
  -  - Return OID value from function
  -  - Check if in bootstrap mode
  -  - Parse potentially schema-qualified name
  -  - Create RangeVar from name list
  -  - Look up relation OID from RangeVar
  -  - Validate OID
  -  - Return error with context
  -  - Convert name list back to string for error messages

- Called from (representative examples):
  -  - Uses this function for class name to OID conversion

## Notes and Other Information
- Accepts both simple and schema-qualified relation names (e.g., "mytable" or "public.mytable")
- Uses current search path for name resolution when schema is not specified
- In bootstrap mode, only numeric OIDs are accepted, not names
- The function does not lock the relation during lookup for performance reasons
- Provides detailed error messages when relations don't exist
- Part of the regtype family of input/output functions for database object references
- Returns 0 (InvalidOid) for dash input, following PostgreSQL conventions for "unknown" values