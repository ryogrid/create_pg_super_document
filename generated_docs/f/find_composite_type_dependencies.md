# find_composite_type_dependencies

## Location
[src/backend/commands/tablecmds.c:6738-6895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6738-L6895)

## Overview
Recursively checks if a given type is being used as a column in any table, including nested usage through composite types, arrays, domains, and other container types.

## Definition

```c
void
find_composite_type_dependencies(Oid typeOid, Relation origRelation,
								 const char *origTypeName)
```
## Detailed Description
This function performs a comprehensive dependency analysis to determine if a specified type (identified by typeOid) is used as a column type anywhere in the database schema. It recursively follows dependencies through container types like arrays, domains, ranges, and composite types to find all potential uses. The function is primarily used during ALTER TYPE operations to prevent modifications that would break existing table columns.

The function scans the pg_depend system catalog to find all objects that depend on the given type, then examines each dependency to determine if it represents a problematic usage (i.e., a stored column in a table). For relations with storage or partitions, any usage of the type results in an error. For views and composite types, the function recursively checks their row types for further dependencies.

The function includes stack depth checking to prevent infinite recursion and uses appropriate locking (AccessShareLock) when examining dependent relations.

## Parameters / Member Variables
- : The OID of the type being checked for dependencies
- : The original relation associated with a rowtype (if applicable), used for error messaging
- : The name of the original type (if not a rowtype), used for error messaging

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)
  - [systable_endscan](../s/systable_endscan.md)
  - RELKIND_HAS_STORAGE
  - RELKIND_HAS_PARTITIONS
  - Form_pg_depend
- Called from (representative examples):
  - [ATRewriteTables](../A/ATRewriteTables.md)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md)
  - [get_rels_with_domain](../g/get_rels_with_domain.md)

## Notes and Other Information
- The function name is somewhat historical; it handles not just composite types but any container type including arrays, ranges, and domains
- Functions and views depending on the type are not considered reasons to reject ALTER operations
- The function assumes system columns are never of types that would cause dependency issues
- For partitioned tables, the function rejects type changes even without stored data due to potential impacts on partitioning rules
- Index expressions that transiently reference the type (without storing it) are considered acceptable dependencies