# AlterTypeRecurse

## Location
[src/backend/commands/typecmds.c:4563-4707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L4563-L4707)

## Overview
A recursive function that applies type property changes to a base type and automatically propagates appropriate changes to its array type and all domains built on top of it.

## Definition

```c
static void
AlterTypeRecurse(Oid typeOid, bool isImplicitArray,
				 HeapTuple tup, Relation catalog,
				 AlterTypeRecurseParams *atparams)
```
## Detailed Description
AlterTypeRecurse performs the actual catalog updates for type property modifications and ensures consistency across related types through recursive propagation. It updates the pg_type tuple for the specified type, regenerates type dependencies, and then recursively processes the associated array type (for typmod functions only) and all domains that use this type as their base. The function implements PostgreSQL's type inheritance model where domains inherit most properties from their base types, while arrays inherit only typmod-related functions.

## Parameters / Member Variables
- : OID of the type being modified
- : Boolean flag indicating if this is an internal call for processing an array type
- : HeapTuple containing the current pg_type row for the type
- : Open relation handle for the pg_type catalog
- : AlterTypeRecurseParams structure containing all the property changes to apply

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - InvokeObjectPostAlterHook
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [AlterTypeRecurse](AlterTypeRecurse.md) (recursive call)
- Called from (representative examples):
  - [AlterType](AlterType.md)
  - [AlterTypeRecurse](AlterTypeRecurse.md) (recursive call)

## Notes and Other Information
- Includes stack depth checking to prevent overflow during deep recursion
- Updates only the pg_type attributes that are flagged for change in atparams
- Rebuilds type dependencies after catalog updates to maintain referential integrity
- Arrays inherit only typmodin and typmodout functions from their base type
- Domains inherit storage, send, and analyze functions but not receive, typmod, or subscript functions
- Uses a system catalog scan to find all domains with the current type as their base type
- Handles race conditions gracefully - concurrent domain creation might be missed but can be fixed by re-running the ALTER TYPE command
- Automatically filters the inheritance for domains by clearing flags for non-inherited properties