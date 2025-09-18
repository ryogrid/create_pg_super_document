# repairTableAttrDefLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1090-1097](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1090-L1097)

## Overview
Repairs circular dependencies for table attribute default loops by removing the dependency from the attribute default object to its parent table.

## Definition
static void repairTableAttrDefLoop(DumpableObject *tableobj, DumpableObject *attrdefobj)

## Detailed Description
This function is part of PostgreSQL's pg_dump dependency resolution system. It specifically handles circular dependencies that occur between tables and their attribute default constraints. When a circular dependency is detected involving a table and its attribute default, this function breaks the loop by removing the attribute default's dependency on the table. The comment in the source indicates that attribute defaults behave exactly the same as CHECK constraints for dependency resolution purposes.

## Parameters / Member Variables
- : Pointer to the DumpableObject representing the table involved in the circular dependency
- : Pointer to the DumpableObject representing the attribute default constraint that needs its dependency removed

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md)
  - DumpableObject (struct type)
- Called from (representative examples):
  - [repairDependencyLoop](repairDependencyLoop.md) (at pg_dump_sort.c:1312)
  - [repairDependencyLoop](repairDependencyLoop.md) (at pg_dump_sort.c:1320)

## Notes and Other Information
- This is a static function within pg_dump_sort.c, indicating it's only used internally within the dependency sorting module
- The function follows the same pattern as CHECK constraint loop repair, as noted in the source comments
- It's part of a larger system for breaking circular dependencies in pg_dump's object dependency graph
- The repair is accomplished by simply removing one direction of the dependency relationship