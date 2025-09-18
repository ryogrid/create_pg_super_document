# IsSystemClass

## Location
[src/backend/catalog/catalog.c:85-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L85-L102)

## Overview
IsSystemClass determines whether a relation is a system relation by checking if it's either a catalog relation or a toast table, using the relation's OID and Form_pg_class tuple directly.

## Definition
```c
bool IsSystemClass(Oid relid, Form_pg_class reltuple)
```

## Detailed Description
This function serves as the core implementation for system relation identification in PostgreSQL. It takes a relation OID and a Form_pg_class tuple as direct parameters, making it suitable for use when the relation is not already open or when working with pg_class entries directly from catalog scans.

The function implements a two-part check: first testing if the relation is a catalog relation using IsCatalogRelationOid (which is optimized for performance), and then checking if it's a toast table using IsToastClass. This approach ensures both system catalogs and toast tables are properly identified as system relations.

## Parameters / Member Variables
- `relid`: The OID of the relation to be checked
- `reltuple`: A Form_pg_class tuple containing the relation's metadata from pg_class

## Dependencies
- Functions called/Symbols referenced:
  - [IsCatalogRelationOid](IsCatalogRelationOid.md)
  - [IsToastClass](IsToastClass.md)
  - Form_pg_class (type)
- Called from (representative examples):
  - [IsSystemRelation](IsSystemRelation.md)
  - [pg_class_aclmask_ext](../p/pg_class_aclmask_ext.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [RangeVarCallbackForPolicy](../R/RangeVarCallbackForPolicy.md)
  - [truncate_check_rel](../t/truncate_check_rel.md)

## Notes and Other Information
- More efficient than IsSystemRelation when you already have the Form_pg_class tuple
- Uses IsCatalogRelationOid first as it's faster than the toast table check
- Designed for use cases where the relation doesn't need to be opened
- Commonly used in callback functions and catalog scanning operations
- The function is located in src/backend/catalog/catalog.c:85-102