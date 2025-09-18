# getFormattedTypeName

## Location
src/bin/pg_dump/pg_dump.c: 18942 - 18992

## Overview
Retrieves a nicely-formatted type name for a given type OID, with caching support and special handling for zero OID values.

## Definition


## Detailed Description
This function converts a PostgreSQL type OID into a human-readable, formatted type name using the pg_catalog.format_type() function. It includes caching mechanism to avoid repeated database queries for the same type, and provides special handling for zero OID values based on options. The function does not guarantee schema-qualified output, so it should not be used for CREATE or ALTER command target names. The result is cached in the TypeInfo record and must not be freed by the caller.

## Parameters / Member Variables
- : Archive pointer for database connection context
- : PostgreSQL Object Identifier for the type
- : OidOptions flags controlling special behaviors (zeroAsStar, zeroAsNone)

## Dependencies
- Functions called/Symbols referenced:
  - findTypeByOid
  - createPQExpBuffer
  - appendPQExpBuffer
  - ExecuteSqlQueryForSingleRow
  - PQgetvalue
  - pg_strdup
  - PQclear
  - destroyPQExpBuffer
- Types used:
  - TypeInfo
  - OidOptions
  - zeroAsStar
  - zeroAsNone
- Called from (representative examples):
  - dumpBaseType
  - format_function_signature
  - dumpFunc
  - dumpCast
  - dumpTransform
  - format_aggregate_signature
  - dumpTableSchema

## Notes and Other Information
- Results are cached in TypeInfo->ftypname to avoid repeated queries
- Special handling for zero OID: returns "*" (zeroAsStar) or "NONE" (zeroAsNone)
- Uses PostgreSQL's format_type() function which already handles quoting
- Memory management: result is owned by TypeInfo cache or leaked if no TypeInfo exists
- Not suitable for generating target names in DDL commands due to potential lack of schema qualification