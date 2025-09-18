# buildMatViewRefreshDependencies

## Location
[src/bin/pg_dump/pg_dump.c:2899-3013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2899-L3013)

## Overview
Establishes dependency relationships between materialized view refresh operations to ensure proper dump ordering when materialized views depend on other materialized views.

## Definition


## Detailed Description
This function builds dependency relationships for materialized view refresh operations by querying the database to find all materialized views that depend on other materialized views through rewrite rules. It uses a recursive CTE to traverse the dependency chain and ensures that when dumping, a materialized view's refresh operation depends on the refresh operations of all materialized views it references. The function also propagates the 'populated' status, marking dependent materialized views as unpopulated if any of their dependencies are unpopulated.

## Parameters / Member Variables
- : Pointer to Archive structure representing the dump output context

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Types referenced:
  - [Archive](../A/Archive.md)
  - PQExpBuffer
  - PGresult
  - [CatalogId](../C/CatalogId.md)
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
  - RELKIND_MATVIEW
  - RELKIND_VIEW
  - DO_TABLE
  - DO_REFRESH_MATVIEW
  - PGRES_TUPLES_OK
- Called from:
  - [main](../m/main.md)

## Notes and Other Information
- Only processes databases with PostgreSQL version 9.3 or later (when materialized views were introduced)
- Uses a complex recursive SQL query to find transitive dependencies between materialized views
- Handles dependency chains where materialized view A depends on B, and B depends on C
- Properly handles mixed dependencies involving both materialized views and regular views
- Ensures that unpopulated status propagates through the dependency chain
- Must be called after all objects are created but before they are sorted for dumping
- Critical for maintaining data consistency when restoring materialized views that reference other materialized views