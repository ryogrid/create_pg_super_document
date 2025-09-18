# dumpType

## Location
src/bin/pg_dump/pg_dump.c: 10920 - 10950

## Overview
Routes user-defined data types to their appropriate specialized dump functions based on the PostgreSQL type system classification.

## Definition


## Detailed Description
The  function serves as a dispatcher for dumping PostgreSQL user-defined data types. It examines the  field of the TypeInfo structure to determine the specific type category and calls the corresponding specialized dump function. The function handles the major PostgreSQL type categories: base types, domains, composite types, enumerations, range types, and pseudo types.

The function includes validation logic that logs a warning if it encounters an invalid or unexpected type type. For pseudo types, it only dumps undefined types (where  is false), as defined pseudo types are typically built-in types that don't need to be recreated.

## Parameters / Member Variables
- : Archive structure representing the dump destination and containing connection/output information
- : Pointer to TypeInfo structure containing type metadata including the type classification () and definition status

## Dependencies
- Functions called/Symbols referenced:
  - dumpBaseType
  - dumpDomain
  - dumpCompositeType
  - dumpEnumType
  - dumpRangeType
  - dumpUndefinedType
  - pg_log_warning
- Called from (representative examples):
  - dumpDumpableObject (in pg_dump.c:10544)
  - crashDumpHandler (in src/backend/port/win32/crashdump.c - multiple locations)

## Notes and Other Information
- Skips processing entirely in data-only dump mode ()
- Handles six major PostgreSQL type categories: base, domain, composite, enum, range, and pseudo types
- Only processes undefined pseudo types, as defined pseudo types are typically system types
- Includes error logging for invalid or unrecognized type classifications
- Each type category has its own specialized dump function for handling type-specific SQL generation
- Part of the broader PostgreSQL pg_dump type system that ensures proper recreation of custom data types