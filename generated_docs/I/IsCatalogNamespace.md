# IsCatalogNamespace

## Location
[src/backend/catalog/catalog.c:212-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L212-L229)

## Overview
Determines whether a given namespace OID represents the pg_catalog system schema where PostgreSQL's built-in system tables and functions reside.

## Definition
```c
bool IsCatalogNamespace(Oid namespaceId)
```

## Detailed Description
IsCatalogNamespace is a simple utility function that checks if a namespace OID corresponds to PostgreSQL's system catalog namespace (pg_catalog). The pg_catalog namespace is special as it contains all of PostgreSQL's built-in system tables, views, functions, and data types.

The function performs a direct comparison against the PG_CATALOG_NAMESPACE constant (OID 11) without requiring any catalog access, making it very efficient. This function is implemented as a function rather than a macro to avoid the need to include catalog/pg_namespace.h in many source files.

## Parameters
- `namespaceId`: The OID of the namespace to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_CATALOG_NAMESPACE (constant: OID 11, representing the pg_catalog namespace)
- Called from (representative examples):
  - [heap_create](../h/heap_create.md) (src/backend/catalog/heap.c:321)
  - [check_publication_add_schema](../c/check_publication_add_schema.md) (src/backend/catalog/pg_publication.c:101)
  - [AlterTableMoveAll](../A/AlterTableMoveAll.md) (src/backend/commands/tablecmds.c:15470)
  - [RelationBuildLocalRelation](../R/RelationBuildLocalRelation.md) (src/backend/utils/cache/relcache.c:3678)

## Notes and Other Information
- Does not perform any catalog accesses, making it very fast
- Implemented as a function rather than a macro to reduce header dependencies
- Used in various catalog operations, DDL commands, and relation cache management
- The pg_catalog namespace contains PostgreSQL's system catalog tables and built-in functions
- PG_CATALOG_NAMESPACE is defined as OID 11 in pg_namespace.dat
- Located in src/backend/catalog/catalog.c at lines 212-229