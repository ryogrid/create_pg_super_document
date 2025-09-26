# getDependencies

## Location
[src/bin/pg_dump/pg_dump.c:18545-18697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18545-L18697)

## Overview
Obtains dependency data from PostgreSQL catalogs to establish ordering relationships between database objects during dump and restore operations.

## Definition
```c
static void getDependencies(Archive *fout)
```

## Detailed Description
This function queries the pg_depend system catalog to collect dependency information between database objects. It processes various types of dependencies except PIN ('p') and EXTENSION ('e') dependencies, which are handled elsewhere. The function handles special cases for pg_amop and pg_amproc entries by translating their dependencies to their parent opfamily objects.

Key behaviors:
- Ignores sub-object columns to treat column dependencies as table dependencies
- Handles special dependency translation for pg_amop and pg_amproc entries to opfamilies
- Excludes internal dependencies and self-dependencies for opfamily entries
- Reverses dependency direction for composite type-table relationships to ensure proper ordering
- Marks objects with extension dependencies ('x' type) for special handling
- Uses ordered results to optimize object lookups when processing multiple dependencies

## Parameters / Member Variables
- `fout`: Archive context for the dump operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - pg_log_warning (in debug builds)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:983)

## Notes and Other Information
- Static function, only accessible within pg_dump.c
- Failure to find objects mentioned in pg_depend is expected and handled gracefully (e.g., TOAST tables)
- Special handling for composite type dependencies ensures proper DROP ordering matches dump restoration needs
- Uses complex UNION query to handle opfamily dependency translation
- Results are ordered by classid and objid for optimization
- Extension dependencies ('x' type) are marked but still processed for ordering purposes
- Debug warnings for missing objects are conditionally compiled (NOT_USED)