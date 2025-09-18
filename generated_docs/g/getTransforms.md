# getTransforms

## Location
[src/bin/pg_dump/pg_dump.c:8714-8804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8714-L8804)

## Overview
Retrieves basic information about every transform in the PostgreSQL system for use by pg_dump, handling version compatibility for features introduced in PostgreSQL 9.5.

## Definition


## Detailed Description
The  function queries the  system catalog to retrieve information about all transform definitions in the database. Transforms define how to convert data types to and from procedural languages (introduced in PostgreSQL 9.5). The function includes version checking to ensure compatibility, returning NULL for PostgreSQL versions prior to 9.5.

For each transform found, it creates a  structure containing the type OID, language OID, and function OIDs for both directions of conversion (fromsql and tosql). The function constructs descriptive names by concatenating the type name and language name for sorting purposes.

## Parameters / Member Variables
- : Archive pointer for the pg_dump operation, used for executing SQL queries and version checking
- : Output parameter that receives the number of transforms found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - TransformInfo
  - pg_malloc
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - [get_language_name](get_language_name.md)
  - initPQExpBuffer
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Only available in PostgreSQL 9.5 and later; returns NULL for earlier versions
- Queries both fromsql and tosql function OIDs, casting them explicitly to oid type
- Transform names are constructed by concatenating type and language names for sorting
- Results are ordered by type OID, then language OID (ORDER BY 3,4)
- If type or language information cannot be found, the transform name remains empty
- Each transform's dumpability is determined by selectDumpableObject()
- Memory is properly managed by freeing the language name after use