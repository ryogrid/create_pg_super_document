# record_object_address_dependencies

## Location
[src/backend/catalog/dependency.c:2742-2760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2742-L2760)

## Overview
Records multiple dependencies from an ObjectAddresses array into the system dependency catalog, after first eliminating any duplicate entries.

## Definition


## Detailed Description
The  function serves as a high-level interface for recording multiple database object dependencies efficiently. It acts as a wrapper around the lower-level dependency recording mechanisms, adding an important preprocessing step to eliminate duplicate dependencies before recording.

The function performs two key operations:
1. **Duplicate elimination**: Calls  to remove any redundant entries from the ObjectAddresses array
2. **Batch dependency recording**: Delegates the actual dependency recording to  with the cleaned array

This approach ensures optimal performance by avoiding redundant dependency records in the system catalogs while maintaining the integrity of the dependency graph.

## Parameters / Member Variables
- : Pointer to the ObjectAddress that depends on the referenced objects
- : Pointer to ObjectAddresses array containing all objects that the depender depends on
- : DependencyType enum value specifying the type of dependency relationship (e.g., NORMAL, AUTO, INTERNAL)

## Dependencies
- Functions called/Symbols referenced:
  - [eliminate_duplicate_dependencies](../e/eliminate_duplicate_dependencies.md) (deduplication function)
  - [recordMultipleDependencies](recordMultipleDependencies.md) (batch dependency recording function)
  - ObjectAddresses (struct type)
  - DependencyType (enum type)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (src/backend/catalog/heap.c:1484)
  - index_create (src/backend/catalog/index.c:1148, 1189)
  - [AggregateCreate](../A/AggregateCreate.md) (src/backend/catalog/pg_aggregate.c:808)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md) (src/backend/catalog/pg_constraint.c:281, 356)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:640)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md) (src/backend/catalog/pg_type.c:706)

## Notes and Other Information
- This function is widely used throughout the PostgreSQL system during object creation operations
- The duplicate elimination step is crucial for performance, as it prevents redundant entries in pg_depend
- Commonly used when creating complex database objects that may have multiple dependencies (tables, indexes, functions, types, etc.)
- The behavior parameter controls how the dependencies are treated during CASCADE operations
- Part of PostgreSQL's comprehensive dependency management system that ensures referential integrity across database objects