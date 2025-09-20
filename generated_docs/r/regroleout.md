# regroleout

## Location
[src/backend/utils/adt/regproc.c:1601-1632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1601-L1632)

## Overview
Converts a role OID to its corresponding role name string, providing output formatting functionality for the regrole type.

## Definition
```c
Datum regroleout(PG_FUNCTION_ARGS)
```

## Detailed Description
The regroleout function serves as the output function for PostgreSQL's regrole type, converting role OIDs back to human-readable string representations. This function is the counterpart to regrolein and completes the I/O operations for the regrole type system.

The function handles several output scenarios:
- Invalid OID (InvalidOid): Returns the special dash notation "-" to indicate an unknown/invalid role
- Valid role OID: Looks up the role name in pg_authid and returns the properly quoted identifier name
- Non-existent role OID: Falls back to returning the numeric OID value as a string

The function uses proper identifier quoting to ensure that role names containing special characters or keywords are correctly represented. This maintains consistency with PostgreSQL's identifier handling throughout the system.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_CSTRING (returns string values)
  - [GetUserNameFromId](../G/GetUserNameFromId.md) (looks up role name by OID)
  - [quote_identifier](../q/quote_identifier.md) (properly quotes role names for output)
  - NAMEDATALEN (maximum length constant for names)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function registry)

## Notes and Other Information
- Located in src/backend/utils/adt/regproc.c:1601-1632
- Part of the regrole type input/output function suite
- Uses special dash notation "-" for invalid/unknown roles
- Applies proper identifier quoting to handle special characters in role names
- Falls back to numeric OID representation when role lookup fails
- Complements regrolein to provide complete type I/O operations for regrole
- The quoted output ensures that role names can be safely used in SQL contexts