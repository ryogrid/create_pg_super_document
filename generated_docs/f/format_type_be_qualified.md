# format_type_be_qualified

## Location
[src/backend/utils/adt/format_type.c:353-361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L353-L361)

## Overview
Backend function that formats PostgreSQL type names with mandatory schema qualification, ensuring unambiguous type identification in contexts where multiple schemas might contain types with the same name.

## Definition


## Detailed Description
 is a specialized variant of the type formatting functions that enforces schema qualification on type names regardless of the current search_path settings. This ensures that the returned type name is unambiguous and can be resolved correctly even when used in different schema contexts.

The function forces qualification by using the  flag, but makes an exception for SQL keyword type names (like "TIMESTAMP WITH TIME ZONE") that are part of the SQL standard and don't require qualification. This behavior is important for generating portable DDL statements and object descriptions that need to work across different database configurations.

Like other backend functions, it will fail for invalid type OIDs rather than returning fallback values, making it suitable for contexts where type validity is expected.

## Parameters / Member Variables
- : PostgreSQL type OID from pg_type.oid that must be valid

## Dependencies
- Functions called/Symbols referenced:
  -  - Core formatting implementation with forced qualification flag
  -  - Flag constant to enforce schema qualification

- Called from (representative examples):
  -  - Object identity generation for dependency tracking
  -  - Procedure name formatting
  -  - Procedure signature formatting  
  -  - Operator name formatting
  -  - Operator signature formatting

## Notes and Other Information
- Always returns a palloc'd string that must be freed by the caller
- Will throw an error for invalid type OIDs (no graceful fallback)
- Uses typemod = -1 (no type modifier information included)
- Exceptions exist for SQL standard types that don't require qualification
- Critical for generating unambiguous object descriptions and DDL statements
- Commonly used in dependency tracking and object identity operations
- Ensures type names remain valid when moved between different schema contexts