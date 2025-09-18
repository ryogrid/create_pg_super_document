# format_type_extended

## Location
src/backend/utils/adt/format_type.c: 112 - 342

## Overview
Core internal function that generates possibly-qualified PostgreSQL type names with extensive formatting control through flag-based options.

## Definition


## Detailed Description
 is the main workhorse function for PostgreSQL type name formatting. It provides comprehensive control over how type names are presented through a flags parameter. The function handles built-in types with special formatting rules, array types, schema qualification decisions, and various error conditions.

The function implements special formatting for standard PostgreSQL types (like converting FLOAT4OID to "real", INT4OID to "integer") to ensure output matches SQL standard names. For non-standard types, it uses the catalog name with appropriate quoting and qualification.

Key behaviors controlled by flags:
- **FORMAT_TYPE_TYPEMOD_GIVEN**: Include typemod in output even if it's -1
- **FORMAT_TYPE_ALLOW_INVALID**: Return "???" for invalid OIDs instead of erroring  
- **FORMAT_TYPE_INVALID_AS_NULL**: Return NULL for invalid OIDs
- **FORMAT_TYPE_FORCE_QUALIFY**: Always include schema qualification

Array handling is sophisticated - it detects "true" array types while avoiding pseudo-arrays like "name" and checks storage properties to avoid showing internal types like oidvector as oid[].

## Parameters / Member Variables
- : PostgreSQL type OID from pg_type.oid
- : Type modifier value, -1 indicates no specific modifier
- : Bitfield controlling formatting behavior (FORMAT_TYPE_* constants)

## Dependencies
- Functions called/Symbols referenced:
  -  - System catalog lookup for type information
  -  - Determines if type is a genuine array type
  -  - Formats type modifiers for display
  -  - Checks if type is in current search path
  -  - Gets schema name for qualification
  -  - Properly quotes and qualifies identifiers
  -  - Safe string formatting

- Called from (representative examples):
  -  - Primary SQL function interface
  -  - [Backend](../B/Backend.md)-only variant
  -  - Always-qualified variant
  -  - Typemod-included variant
  -  - Object description generation
  -  - Array input processing

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller, or NULL for invalid types when appropriate flags are set
- Contains extensive special-case handling for built-in types to ensure SQL standard compliance
- The array detection logic carefully avoids pseudo-arrays to prevent confusing output
- Schema qualification logic respects search_path unless forced
- Critical for maintaining consistency in pg_dump output and ensuring DDL reconstruction accuracy
- Handles edge cases like bit(-1) vs BIT and bpchar(-1) vs CHARACTER to maintain parser compatibility