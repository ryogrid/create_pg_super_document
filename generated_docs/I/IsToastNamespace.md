# IsToastNamespace

## Location
src/backend/catalog/catalog.c: 230 - 246

## Overview
Determines whether a given namespace OID represents either the global pg_toast namespace or the current session's temporary-toast-table namespace.

## Definition
```c
bool IsToastNamespace(Oid namespaceId)
```

## Detailed Description
IsToastNamespace checks if a namespace OID corresponds to any TOAST (The Oversized-Attribute Storage Technique) namespace that the current session can access. It returns true for two types of namespaces:

1. The global pg_toast namespace (OID 99) - where regular TOAST tables are stored
2. The current session's temporary-toast-table namespace - for TOAST tables associated with temporary tables

The function does not perform catalog accesses and will return false for temporary-toast-table namespaces belonging to other backend sessions. This is intentional as access to other sessions' temporary TOAST tables should be prevented through other mechanisms.

## Parameters
- `namespaceId`: The OID of the namespace to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_TOAST_NAMESPACE (constant: OID 99, representing the global pg_toast namespace)
  - [isTempToastNamespace](../i/isTempToastNamespace.md) (checks if namespace is current session's temporary toast namespace)
- Called from (representative examples):
  - [IsToastRelation](IsToastRelation.md) (src/backend/catalog/catalog.c:185)
  - [IsToastClass](IsToastClass.md) (src/backend/catalog/catalog.c:199)
  - [heap_create](../h/heap_create.md) (src/backend/catalog/heap.c:322)
  - [reindex_index](../r/reindex_index.md) (src/backend/catalog/index.c:3675)
  - [reindex_relation](../r/reindex_relation.md) (src/backend/catalog/index.c:3993)
  - [check_publication_add_schema](../c/check_publication_add_schema.md) (src/backend/catalog/pg_publication.c:101)
  - [AlterTableMoveAll](../A/AlterTableMoveAll.md) (src/backend/commands/tablecmds.c:15473)

## Notes and Other Information
- Does not perform any catalog accesses, making it efficient for frequent use
- Only recognizes the current session's temporary TOAST namespace, not those of other sessions
- For checking temporary TOAST namespaces of other backends, consider using isAnyTempNamespace (which does involve catalog access)
- Part of PostgreSQL's TOAST system for handling oversized attribute values
- PG_TOAST_NAMESPACE is defined as OID 99 in pg_namespace.dat
- Located in src/backend/catalog/catalog.c at lines 230-246