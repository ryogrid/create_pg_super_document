# enum_in

## Location
[src/backend/utils/adt/enum.c:109-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L109-L154)

## Overview
Converts a string representation of an enum value to its internal OID representation for storage and processing.

## Definition


## Detailed Description
This function implements the input conversion for PostgreSQL enum types, transforming textual enum labels into their corresponding internal OID values. It serves as the standard input function for enum types, handling the conversion from human-readable enum labels to the system's internal representation.

The function performs several validation steps: it checks the input string length to prevent buffer overflows, searches the system catalog for the enum value, and ensures the found enum value is safe to use (not an uncommitted value that could cause index corruption). The returned OID comes from pg_enum.oid and represents a system identifier that must be preserved during binary upgrades.

## Parameters / Member Variables
-  (PG_GETARG_CSTRING(0)): The string representation of the enum value to convert
-  (PG_GETARG_OID(1)): The OID of the enum type that this value should belong to
-  (fcinfo->context): Error context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN
  - ereturn
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [check_safe_enum_use](../c/check_safe_enum_use.md)
  - Form_pg_enum
  - PG_RETURN_OID
- Called from (representative examples):
  - No direct references found (called via function manager)

## Notes and Other Information
- This is a PostgreSQL I/O function that follows the standard Datum-returning pattern
- Validates input string length against NAMEDATALEN to prevent Assert failures in SearchSysCache
- Uses the ENUMTYPOIDNAME system cache for efficient enum value lookup
- Calls check_safe_enum_use to prevent use of uncommitted enum values
- The returned OID is crucial for binary upgrade compatibility
- Supports soft error reporting through escontext for better error handling
- Part of the basic I/O support for enum types in PostgreSQL