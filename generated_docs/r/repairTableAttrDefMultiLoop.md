# repairTableAttrDefMultiLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1098-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1098-L1112)

## Overview
Repairs multi-loop circular dependencies involving table attribute defaults by reversing dependency direction and marking the attribute default for separate dumping.

## Definition
static void repairTableAttrDefMultiLoop(DumpableObject *tableobj, DumpableObject *attrdefobj)

## Detailed Description
This function handles more complex circular dependency scenarios involving table attribute defaults where simple dependency removal isn't sufficient. It employs a three-step repair strategy: first removing the table's dependency on the attribute default, then marking the attribute default to be dumped separately (setting the separate flag), and finally re-establishing the dependency in the opposite direction (attribute default depends on table). This approach ensures proper ordering while maintaining the necessary relationships for correct database restoration.

## Parameters / Member Variables
- `tableobj`: Pointer to the DumpableObject representing the table involved in the multi-loop dependency
- `attrdefobj`: Pointer to the DumpableObject representing the attribute default constraint that needs dependency restructuring

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - DumpableObject (struct type)
  - [AttrDefInfo](../A/AttrDefInfo.md) (struct type)
- Called from (representative examples):
  - [repairDependencyLoop](repairDependencyLoop.md) (at pg_dump_sort.c:1353)

## Notes and Other Information
- This is a static function within pg_dump_sort.c for internal dependency sorting use
- More sophisticated than repairTableAttrDefLoop, handling complex multi-loop scenarios
- Uses the AttrDefInfo struct's separate flag to ensure independent dumping of the attribute default
- The dependency reversal strategy maintains correctness while breaking circular references
- Part of pg_dump's comprehensive dependency resolution system for complex database schemas

## Simplified Source

```c
static void repairTableAttrDefMultiLoop(DumpableObject *tableobj,
                                       DumpableObject *attrdefobj) {
    // Break the dependency cycle: remove table's dependency on attribute default
    removeObjectDependency(tableobj, attrdefobj->dumpId);

    // Mark attribute default as needing its own separate dump operation
    ((AttrDefInfo *) attrdefobj)->separate = true;

    // Restore attribute default's dependency on table (ensures correct order)
    addObjectDependency(attrdefobj, tableobj->dumpId);
}
```