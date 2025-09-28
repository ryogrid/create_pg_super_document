# IsSystemRelation

## Location
[src/backend/catalog/catalog.c:73-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L73-L84)

## Overview
IsSystemRelation determines whether a given relation (table) is either a system catalog or a toast table that should be treated as a system relation for protection purposes.

## Definition
```c
bool IsSystemRelation(Relation relation)
```

## Detailed Description
This function identifies relations that should be subject to system-level protection mechanisms in PostgreSQL. Unlike IsCatalogRelation which only identifies true system catalogs, IsSystemRelation extends this classification to include toast tables of user relations. This broader classification is used primarily for access control and schema modification restrictions.

The function acts as a wrapper around IsSystemClass, extracting the relation OID and Form_pg_class tuple from the Relation structure. It's designed to be lightweight and does not perform any catalog accesses, which is important for performance in contexts where catalog lookups would be problematic.

## Parameters / Member Variables
- `relation`: A Relation structure representing the table/relation to be checked

## Dependencies
- Functions called/Symbols referenced:
  - [IsSystemClass](IsSystemClass.md)
  - RelationGetRelid (macro to extract OID from relation)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)
  - [index_create](../i/index_create.md)
  - [ATRewriteTables](../A/ATRewriteTables.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)
  - [get_relation_info](../g/get_relation_info.md)

## Notes and Other Information
- This function is specifically designed for checking allow_system_table_mods restrictions
- Toast tables of user relations are treated as "system relations" for protection purposes
- Does not perform catalog accesses, making it safe to use in contexts where catalog lookups are not possible
- For purposes other than protection/access control, consider using IsCatalogRelation instead
- The function is located in src/backend/catalog/catalog.c:73-84

## Simplified Source

```c
// Simplified version of IsSystemRelation
bool
IsSystemRelation(Relation relation)
{
    // Delegate to IsSystemClass with relation OID and pg_class tuple
    return IsSystemClass(RelationGetRelid(relation), relation->rd_rel);
}
```

Key simplifications made:
- Added comment explaining the delegation to IsSystemClass
- Function is already very simple, essentially a wrapper
- Preserved the exact functionality while clarifying purpose