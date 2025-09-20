# drop_parent_dependency

## Location
[src/backend/commands/tablecmds.c:16434-16485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16434-L16485)

## Overview
Removes dependency entries from pg_depend catalog that were created during table inheritance or typed table relationships.

## Definition

```c
static void
drop_parent_dependency(Oid relid, Oid refclassid, Oid refobjid,
					   DependencyType deptype)
```
## Detailed Description
drop_parent_dependency is a utility function that removes specific dependency entries from the pg_depend system catalog. It searches for and deletes dependency records that match the specified criteria, effectively breaking the dependency relationship between a child table and its parent (either inheritance or typed table relationships). The function scans pg_depend using the DependDependerIndexId index to efficiently locate matching dependency entries and removes them using CatalogTupleDelete. This is used when breaking inheritance relationships (INHERITS) or typed table relationships (OF type).

## Parameters / Member Variables
- : Object ID of the dependent relation (child table)
- : Class ID of the referenced object (RelationRelationId for inheritance, TypeRelationId for typed tables)
- : Object ID of the referenced object (parent table or type)
- : Type of dependency to remove (e.g., DEPENDENCY_NORMAL, DEPENDENCY_AUTO)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
- Called from (representative examples):
  - [RemoveInheritance](../R/RemoveInheritance.md)
  - [ATExecAddOf](../A/ATExecAddOf.md)
  - [ATExecDropOf](../A/ATExecDropOf.md)

## Notes and Other Information
- Used for both inheritance relationships (CREATE/ALTER TABLE INHERITS) and typed table relationships (CREATE/ALTER TABLE OF)
- Scans pg_depend using a three-part key: classid, objid, and objsubid for efficient lookup
- Operates under RowExclusiveLock on the pg_depend catalog to ensure consistency
- Handles dependencies created by StoreCatalogInheritance1 and heap_create_with_catalog functions
- The function comment notes there's no convenient way to remove these dependencies, hence the manual scanning approach
- Matches dependencies precisely using all relevant fields including refclassid, refobjid, refobjsubid, and deptype