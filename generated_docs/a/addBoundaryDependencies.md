# addBoundaryDependencies

## Location
src/bin/pg_dump/pg_dump.c: 18722 - 18837

## Overview
Adds dependency relationships between database objects and dump section boundary objects to enforce the three-phase dump structure (pre-data, data, post-data).

## Definition
```c
static void addBoundaryDependencies(DumpableObject **dobjs, int numObjs, DumpableObject *boundaryObjs)
```

## Detailed Description
This function enforces the logical separation of database dump output into three distinct phases by adding appropriate dependencies between database objects and boundary objects:

1. **Pre-data phase**: Schema definitions (tables, functions, types, etc.) that must be created before data loading
2. **Data phase**: Actual table data, sequence values, and large objects
3. **Post-data phase**: Objects that depend on data being present (indexes, constraints, triggers, etc.)

The function iterates through all DumpableObjects and classifies them by type, then adds dependencies to ensure:
- Pre-data objects come before the pre-data boundary
- Data objects come between the boundaries (after pre-data, before post-data)
- Post-data objects come after the post-data boundary
- Some objects (rules, constraints) are only classified as post-data if dumped separately

## Parameters / Member Variables
- `dobjs`: Array of pointers to all DumpableObject instances in the dump
- `numObjs`: Number of objects in the dobjs array  
- `boundaryObjs`: Array containing the pre-data and post-data boundary objects

## Dependencies
- Functions called/Symbols referenced:
  - [addObjectDependency](addObjectDependency.md)
- Object types classified:
  - Pre-data: DO_NAMESPACE, DO_EXTENSION, DO_TYPE, DO_FUNC, DO_TABLE, etc.
  - Data: DO_TABLE_DATA, DO_SEQUENCE_SET, DO_LARGE_OBJECT, etc.
  - Post-data: DO_INDEX, DO_TRIGGER, DO_CONSTRAINT, DO_POLICY, etc.
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:1004)

## Notes and Other Information
- Static function, only accessible within pg_dump.c
- Object type classification must match SECTION_xxx values used in subsequent ArchiveEntry calls
- Rules and constraints have conditional classification - only post-data if dumped separately
- The post-data boundary depends on the pre-data boundary to maintain proper ordering
- Critical for ensuring dump output can be restored correctly without dependency violations
- Handles special cases where the same object type may belong to different phases depending on context