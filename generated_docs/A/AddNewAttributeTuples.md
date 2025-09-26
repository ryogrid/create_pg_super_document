# AddNewAttributeTuples

## Location
[src/backend/catalog/heap.c:821-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L821-L895)

## Overview
Registers a new relation's schema by adding attribute tuples to pg_attribute and establishing necessary dependencies on data types and collations.

## Definition

```c
static void
AddNewAttributeTuples(Oid new_rel_oid,
					  TupleDesc tupdesc,
					  char relkind)
```
## Detailed Description
AddNewAttributeTuples is a high-level catalog management function that handles the complete registration of a relation's attribute schema in the PostgreSQL system catalogs. It serves as a wrapper around InsertPgAttributeTuples, adding crucial dependency management and system attribute handling.

The function performs three main operations: first, it inserts the user-defined attributes from the provided TupleDesc into pg_attribute; second, it records dependencies between each attribute and its data type and collation to ensure proper cascading behavior during DROP operations; third, for appropriate relation kinds, it adds system attributes (like oid, ctid, xmin, xmax, etc.) that are automatically present in PostgreSQL tables.

The function intelligently handles different relation types, skipping system attributes for views and composite types where they are not applicable. It also optimizes dependency recording by skipping the default collation since it's pinned and doesn't require explicit dependency tracking.

## Parameters / Member Variables
- : OID of the newly created relation for which attributes are being added
- : TupleDesc containing the attribute definitions to be registered in pg_attribute
- : Character indicating the kind of relation (table, view, composite type, etc.) to determine system attribute handling

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md)
  - ObjectAddressSubSet
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [CreateTupleDesc](../C/CreateTupleDesc.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)

## Notes and Other Information
- This function is static and primarily used during relation creation as part of the heap_create_with_catalog process
- System attributes are only added for regular tables and other relation types that support them, excluding views and composite types
- The function establishes DEPENDENCY_NORMAL relationships between attributes and their types/collations for proper cascade behavior
- Dependencies on pinned system types (for system attributes) are not recorded since they cannot be dropped
- The function handles both RowExclusiveLock acquisition and proper cleanup of catalog resources