# repairMatViewBoundaryMultiLoop

## Location
[src/bin/pg_dump/pg_dump_sort.c:1012-1034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump_sort.c#L1012-L1034)

## Overview
Repairs circular dependencies involving materialized views in pg_dump by removing pre-data phase constraints and postponing matview definitions to the post-data phase as a stopgap solution.

## Definition
```c
static void repairMatViewBoundaryMultiLoop(DumpableObject *boundaryobj, DumpableObject *nextobj)
```

## Detailed Description
This function addresses multi-object dependency loops involving materialized views, which cannot be resolved using the rule-splitting approach used for regular views. As a workaround, it:

1. Removes the boundary object's dependency on the next object in the loop
2. If the next object is a materialized view (relkind == RELKIND_MATVIEW), marks it as postponed

The postponement mechanism moves the matview definition from the pre-data phase to the post-data phase, which is sufficient to handle common cases like matviews that depend on unique indexes (e.g., when using GROUP BY clauses).

The function may be called multiple times for interconnected matviews, progressively marking all affected matviews as postponed. This works because all materialized views initially have pre-data dependencies, ensuring each one gets processed.

## Parameters / Member Variables
- `boundaryobj`: Pointer to the DumpableObject that represents the boundary/source of the dependency in the loop
- `nextobj`: Pointer to the DumpableObject that follows the boundary object in the dependency cycle (may be a matview, matview rowtype, or other related object)

## Dependencies
- Functions called/Symbols referenced:
  - [removeObjectDependency](removeObjectDependency.md) (removes the problematic dependency relationship)
  - DO_TABLE (constant identifying table-type objects)
  - RELKIND_MATVIEW (constant identifying materialized view relation kind)
  - DumpableObject (base structure for dumpable database objects)
  - [TableInfo](../T/TableInfo.md) (structure containing table/view-specific information)
- Called from:
  - [repairDependencyLoop](repairDependencyLoop.md) (main dependency loop repair dispatcher)

## Notes and Other Information
- This is explicitly described as a "stopgap" solution since the proper rule-splitting approach used for regular views doesn't work for materialized views
- The function handles cases where matviews depend on unique indexes, commonly occurring with GROUP BY clauses
- The "next object" may not be the matview itself but could be related objects like the matview's rowtype
- Multiple calls may occur to handle cascading dependencies between interconnected matviews
- The postponed_def flag moves the CREATE MATERIALIZED VIEW statement to the post-data phase
- This approach is less elegant than the view-rule splitting but necessary due to matview implementation constraints

## Simplified Source

```c
static void
repairMatViewBoundaryMultiLoop(DumpableObject *boundaryobj,
                              DumpableObject *nextobj)
{
    // Break dependency in the loop
    removeObjectDependency(boundaryobj, nextobj->dumpId);

    // If next object is a materialized view, postpone its definition
    if (nextobj->objType == DO_TABLE) {
        TableInfo *nextinfo = (TableInfo *) nextobj;

        if (nextinfo->relkind == RELKIND_MATVIEW)
            nextinfo->postponed_def = true;  // Move to post-data phase
    }
}
```