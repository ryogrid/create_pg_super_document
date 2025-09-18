# repairTableConstraintLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1056-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1056-L1072)

## Overview
Repairs simple circular dependencies between tables and their CHECK constraints in pg_dump by removing the automatic constraint-to-table dependency while preserving the explicit table-to-constraint dependency.

## Definition
```c
static void repairTableConstraintLoop(DumpableObject *tableobj, DumpableObject *constraintobj)
```

## Detailed Description
This function handles a specific type of two-object dependency cycle that occurs between tables and their CHECK constraints. The circular dependency arises because:

1. Tables explicitly depend on their CHECK constraints (pg_dump makes tables depend on their constraints)
2. CHECK constraints have an automatic dependency on their parent table (inherent in the PostgreSQL system)

When only these two objects are involved in the loop (no other objects), the function resolves the cycle by simply removing the automatic constraint-to-table dependency. This leaves only the explicit table-to-constraint dependency, ensuring the constraint will be dumped as part of the table definition rather than as a separate object.

This is the simplest form of constraint loop repair, used when no other objects complicate the dependency chain.

## Parameters / Member Variables
- `tableobj`: Pointer to the DumpableObject representing the table involved in the dependency loop
- `constraintobj`: Pointer to the DumpableObject representing the CHECK constraint that creates the circular dependency with the table

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md) (removes the automatic constraint-to-table dependency)
  - DumpableObject (base structure for dumpable database objects)
- Called from:
  - [repairDependencyLoop](repairDependencyLoop.md) (main dependency loop repair dispatcher, called twice at lines 1272 and 1281)

## Notes and Other Information
- This function only handles simple two-object loops between tables and CHECK constraints
- For more complex loops involving additional objects, repairTableConstraintMultiLoop is used instead
- The solution preserves the table-to-constraint dependency, meaning the constraint remains integrated with the table definition
- This approach avoids making the constraint a separate dumpable object, which would require post-data phase handling
- The automatic dependency being removed is the built-in PostgreSQL dependency that constraints have on their parent tables
- This is the most efficient repair mechanism for simple table-constraint cycles since it requires minimal reorganization of the dump structure