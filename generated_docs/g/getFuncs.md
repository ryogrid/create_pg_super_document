# getFuncs

## Location
[src/bin/pg_dump/pg_dump.c:6607-6805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6607-L6805)

## Overview
The getFuncs function retrieves all user-defined functions from the PostgreSQL system catalogs and returns them in a FuncInfo structure array for use by pg_dump, excluding aggregates and internally dependent functions.

## Definition

```c
structor
	 * functions for range types.  Note this is OK only because the
	 * constructors don't have any dependencies the range type doesn't have;
```
## Detailed Description
This function is part of pg_dump's catalog reading functionality that handles regular functions (non-aggregates). It implements sophisticated filtering logic to determine which functions should be included in the dump. The function constructs complex SQL queries that vary based on PostgreSQL version, filtering out aggregates, internally dependent functions (like range type constructors), and system functions in pg_catalog unless they meet specific criteria.

The filtering criteria include functions used by casts or transforms, functions that are part of extensions in binary-upgrade mode, and functions with custom privileges different from their initial privileges. The function handles version-specific changes in PostgreSQL's function identification (proisagg vs prokind) and supports both older and newer privilege management systems.

Each function is represented by a FuncInfo structure containing comprehensive metadata including OID, name, namespace, language, argument types, return type, owner, and access control information.

## Parameters / Member Variables
- : Archive structure containing connection information and dump configuration options
- : Pointer to integer that will be set to the number of functions found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [pg_malloc0](../p/pg_malloc0.md)
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
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Excludes aggregate functions (handled separately by getAggregates)
- Filters out internally dependent functions like range type constructors
- System functions in pg_catalog are excluded unless they meet special criteria (used by casts/transforms, extension members, custom privileges)
- Uses version-specific SQL queries to handle PostgreSQL evolution (proisagg vs prokind field)
- Supports binary upgrade mode with special handling for extension-dependent functions
- Handles both parameterized and non-parameterized functions with proper argument type parsing
- Uses DO_FUNC object type identifier for dump object classification
- Preserves ACL information for functions with custom privileges
- Memory allocation uses pg_malloc0 for zero-initialized structures