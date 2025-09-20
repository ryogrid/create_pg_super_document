# getAggregates

## Location
[src/bin/pg_dump/pg_dump.c:6460-6606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6460-L6606)

## Overview
The getAggregates function retrieves all user-defined aggregate functions from the PostgreSQL system catalogs and returns them in an AggInfo structure array for use by pg_dump.

## Definition

```c
AggInfo *
getAggregates(Archive *fout, int *numAggs)
```
## Detailed Description
This function is part of pg_dump's catalog reading functionality that specifically handles aggregate functions. It constructs and executes a complex SQL query against the pg_proc system catalog to retrieve user-defined aggregates, filtering out system-defined aggregates in pg_catalog unless they have custom privileges. The function handles different PostgreSQL versions, using different aggregate identification methods (proisagg for older versions, prokind = 'a' for PostgreSQL 11+).

The function creates AggInfo structures for each aggregate, populating them with comprehensive metadata including OID, name, namespace, argument types, owner, and access control information. It also handles argument type parsing for aggregates that take parameters and manages ACL (Access Control List) information for privilege management during dump/restore operations.

## Parameters / Member Variables
- : Archive structure containing connection information and dump configuration options
- : Pointer to integer that will be set to the number of aggregates found

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - pg_malloc
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - atoi
  - [parseOidArray](../p/parseOidArray.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function uses version-specific SQL queries to handle changes in PostgreSQL's aggregate identification (proisagg vs prokind)
- System aggregates in pg_catalog are filtered out unless they have custom privileges or are part of extensions during binary upgrades
- ACL information is preserved for aggregates that have custom privileges
- Argument types are parsed from the proargtypes array and stored as separate Oid arrays
- The function handles both parameterized and non-parameterized aggregates
- Uses DO_AGG object type identifier for dump object classification
- Supports binary upgrade mode with special handling for extension-dependent aggregates