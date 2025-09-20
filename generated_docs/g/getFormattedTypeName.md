# getFormattedTypeName

## Location
[src/bin/pg_dump/pg_dump.c:18942-18992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18942-L18992)

## Overview
Retrieves a nicely-formatted type name for a given type OID, with caching support and special handling for zero OID values.

## Definition

```c
static const char *
getFormattedTypeName(Archive *fout, Oid oid, OidOptions opts)
```
## Detailed Description
This function converts a PostgreSQL type OID into a human-readable, formatted type name using the pg_catalog.format_type() function. It includes caching mechanism to avoid repeated database queries for the same type, and provides special handling for zero OID values based on options. The function does not guarantee schema-qualified output, so it should not be used for CREATE or ALTER command target names. The result is cached in the TypeInfo record and must not be freed by the caller.

## Parameters / Member Variables
- : Archive pointer for database connection context
- : PostgreSQL Object Identifier for the type
- : OidOptions flags controlling special behaviors (zeroAsStar, zeroAsNone)

## Dependencies
- Functions called/Symbols referenced:
  - [findTypeByOid](../f/findTypeByOid.md)
  - createPQExpBuffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Types used:
  - [TypeInfo](../T/TypeInfo.md)
  - [OidOptions](../O/OidOptions.md)
  - zeroAsStar
  - zeroAsNone
- Called from (representative examples):
  - [dumpBaseType](../d/dumpBaseType.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpCast](../d/dumpCast.md)
  - [dumpTransform](../d/dumpTransform.md)
  - [format_aggregate_signature](../f/format_aggregate_signature.md)
  - [dumpTableSchema](../d/dumpTableSchema.md)

## Notes and Other Information
- Results are cached in TypeInfo->ftypname to avoid repeated queries
- Special handling for zero OID: returns "*" (zeroAsStar) or "NONE" (zeroAsNone)
- Uses PostgreSQL's format_type() function which already handles quoting
- Memory management: result is owned by TypeInfo cache or leaked if no TypeInfo exists
- Not suitable for generating target names in DDL commands due to potential lack of schema qualification