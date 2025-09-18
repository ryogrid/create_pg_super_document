# get_am_type_oid

## Location
src/backend/commands/amcmds.c: 129 - 162

## Overview
Internal worker function that looks up an access method by name and optionally validates its type, returning the corresponding OID.

## Definition


## Detailed Description
get_am_type_oid serves as a core utility function for access method OID lookups with optional type validation. It searches the pg_am system catalog for an access method by name and can enforce type constraints when specified. The function supports both strict mode (throwing errors for missing access methods) and lenient mode (returning InvalidOid), making it suitable for various use cases throughout the access method subsystem.

## Parameters / Member Variables
- : Name of the access method to look up
- : Expected access method type character ('\0' to skip type validation)
- : If false, throws error when access method not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1: Searches system cache for access method by name
  - CStringGetDatum: Converts C string to Datum for cache lookup
  - GETSTRUCT: Macro to extract structure from heap tuple
  - get_am_type_string: Converts access method type character to string representation
  - ReleaseSysCache: Releases system cache tuple
- Called from (representative examples):
  - get_index_am_oid: Gets OID for index access methods
  - get_table_am_oid: Gets OID for table access methods
  - get_am_oid: Gets OID for any access method type

## Notes and Other Information
- This is a static function used internally within the access method command subsystem
- Provides centralized access method lookup logic with type validation
- Uses system cache for efficient repeated lookups
- Error handling distinguishes between missing access methods and type mismatches
- Location: src/backend/commands/amcmds.c:129-162