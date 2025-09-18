# repairTableConstraintMultiLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1073-1089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1073-L1089)

## Overview
Repairs complex circular dependencies involving tables, their CHECK constraints, and other objects in pg_dump by making the constraint a separately-dumped object and moving it to the post-data phase.

## Definition
```c
static void repairTableConstraintMultiLoop(DumpableObject *tableobj, DumpableObject *constraintobj)
```

## Detailed Description
This function handles multi-object dependency loops involving tables and their CHECK constraints when additional objects complicate the dependency chain. Unlike the simple two-object case handled by repairTableConstraintLoop, this function must break the cycle by separating the constraint from the table definition:

1. Removes the table's explicit dependency on the constraint
2. Marks the constraint as requiring its own separate dump operation
3. Re-establishes the constraint's dependency on the table (ensuring proper order)
4. Moves the constraint to the post-data phase for separate dumping

The function accounts for the possibility that repairTableConstraintLoop() may have been called previously (since findLoop() finds shorter cycles first) and removed the constraint's dependency on the table. In such cases, it explicitly restores this dependency to maintain proper dump ordering.

This approach ensures that the table can be created first, followed by the constraint as a separate ALTER TABLE ADD CONSTRAINT statement in the post-data phase.

## Parameters / Member Variables
- `tableobj`: Pointer to the DumpableObject representing the table involved in the multi-object dependency loop
- `constraintobj`: Pointer to the DumpableObject representing the CHECK constraint that needs to be separated from the table definition

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md) (removes the table-to-constraint dependency)
  - [addObjectDependency](../a/addObjectDependency.md) (establishes constraint-to-table and constraint-to-postdata dependencies)
  - postDataBoundId (global variable marking the post-data phase boundary)
  - DumpableObject (base structure for dumpable database objects)  
  - [ConstraintInfo](../C/ConstraintInfo.md) (structure containing constraint-specific information)
- Called from:
  - [repairDependencyLoop](repairDependencyLoop.md) (main dependency loop repair dispatcher)

## Notes and Other Information
- This function is used when simple constraint loop repair (repairTableConstraintLoop) is insufficient due to additional objects in the dependency cycle
- The separate flag causes the constraint to be dumped as an independent ALTER TABLE ADD CONSTRAINT statement
- Moving the constraint to post-data phase (via postDataBoundId dependency) ensures it's applied after table data is loaded
- The function may restore a previously removed constraint-to-table dependency to maintain proper ordering
- This approach is more complex than the simple loop repair but necessary when multiple objects create intricate dependency webs
- The constraint will be dumped separately from the table definition, potentially affecting dump readability but ensuring successful restoration