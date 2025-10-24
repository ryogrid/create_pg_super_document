# addBoundaryDependencies

## Location
[src/bin/pg_dump/pg_dump.c:18722-18837](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18722-L18837)

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

## Simplified Source

```c
static void addBoundaryDependencies(DumpableObject **dobjs, int numObjs,
                                   DumpableObject *boundaryObjs)
{
    DumpableObject *preDataBound = &boundaryObjs[0];
    DumpableObject *postDataBound = &boundaryObjs[1];

    for (int i = 0; i < numObjs; i++) {
        DumpableObject *obj = dobjs[i];

        switch (obj->objType) {
            // Pre-data objects: schema definitions first
            case DO_NAMESPACE: case DO_TYPE: case DO_FUNC: case DO_TABLE:
            case DO_EXTENSION: case DO_OPERATOR: case DO_COLLATION:
                addObjectDependency(preDataBound, obj->dumpId);
                break;

            // Data objects: between boundaries
            case DO_TABLE_DATA: case DO_SEQUENCE_SET: case DO_LARGE_OBJECT:
                addObjectDependency(obj, preDataBound->dumpId);
                addObjectDependency(postDataBound, obj->dumpId);
                break;

            // Post-data objects: after data is loaded
            case DO_INDEX: case DO_TRIGGER: case DO_POLICY:
            case DO_PUBLICATION: case DO_SUBSCRIPTION:
                addObjectDependency(obj, postDataBound->dumpId);
                break;

            // Special cases: conditionally post-data
            case DO_RULE:
                if (((RuleInfo *) obj)->separate)
                    addObjectDependency(obj, postDataBound->dumpId);
                break;
            case DO_CONSTRAINT:
                if (((ConstraintInfo *) obj)->separate)
                    addObjectDependency(obj, postDataBound->dumpId);
                break;

            // Boundary maintenance
            case DO_POST_DATA_BOUNDARY:
                addObjectDependency(obj, preDataBound->dumpId);
                break;
        }
    }
}
```