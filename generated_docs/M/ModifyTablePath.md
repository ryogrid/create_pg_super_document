# ModifyTablePath

## Location
[src/include/nodes/pathnodes.h:2375-2395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2375-L2395)

## Overview
ModifyTablePath represents a query execution path node for performing INSERT/UPDATE/DELETE/MERGE operations, containing all the information needed to modify table data including target relations, column specifications, and conflict handling.

## Definition
```c
typedef struct ModifyTablePath
{
    Path        path;
    Path       *subpath;                /* Path producing source data */
    CmdType     operation;              /* INSERT, UPDATE, DELETE, or MERGE */
    bool        canSetTag;              /* do we set the command tag/es_processed? */
    Index       nominalRelation;        /* Parent RT index for use of EXPLAIN */
    Index       rootRelation;           /* Root RT index, if partitioned/inherited */
    bool        partColsUpdated;        /* some part key in hierarchy updated? */
    List       *resultRelations;        /* integer list of RT indexes */
    List       *updateColnosLists;      /* per-target-table update_colnos lists */
    List       *withCheckOptionLists;   /* per-target-table WCO lists */
    List       *returningLists;         /* per-target-table RETURNING tlists */
    List       *rowMarks;               /* PlanRowMarks (non-locking only) */
    OnConflictExpr *onconflict;         /* ON CONFLICT clause, or NULL */
    int         epqParam;               /* ID of Param for EvalPlanQual re-eval */
    List       *mergeActionLists;       /* per-target-table lists of actions for MERGE */
    List       *mergeJoinConditions;    /* per-target-table join conditions for MERGE */
} ModifyTablePath;
```

## Detailed Description
ModifyTablePath is a comprehensive path node that encapsulates all aspects of data modification operations in PostgreSQL. It serves as the planning representation for INSERT, UPDATE, DELETE, and MERGE statements, containing detailed information about target tables, column specifications, constraint checking, and conflict resolution. The path includes support for complex scenarios like partitioned tables, inheritance hierarchies, WITH CHECK OPTIONS, RETURNING clauses, and ON CONFLICT handling. Analysis of OnConflictExpr and FDW data collection is deferred to createplan.c for efficiency.

## Parameters / Member Variables
- `path`: Base Path structure with cost estimates, row counts, and execution metadata
- `subpath`: Path node that produces the source data for the modification operation
- `operation`: Type of DML operation (CMD_INSERT, CMD_UPDATE, CMD_DELETE, or CMD_MERGE)
- `canSetTag`: Whether this operation should set the command tag and es_processed counter
- `nominalRelation`: Range table index of the parent relation for EXPLAIN purposes
- `rootRelation`: Range table index of the root relation in partitioned/inherited table hierarchies
- `partColsUpdated`: Flag indicating if any partitioning key columns are being updated
- `resultRelations`: List of range table indexes for all target relations
- `updateColnosLists`: Per-target-table lists of column numbers being updated (UPDATE only)
- `withCheckOptionLists`: Per-target-table lists of WITH CHECK OPTION constraints
- `returningLists`: Per-target-table target lists for RETURNING clauses
- `rowMarks`: List of PlanRowMark structures for non-locking row marking
- `onconflict`: ON CONFLICT expression for handling constraint violations, or NULL
- `epqParam`: Parameter ID for EvalPlanQual re-evaluation during concurrent modifications
- `mergeActionLists`: Per-target-table lists of actions for MERGE operations
- `mergeJoinConditions`: Per-target-table join conditions for MERGE operations

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - CmdType (operation type enum)
  - [OnConflictExpr](../O/OnConflictExpr.md) (conflict handling)
  - [List](../L/List.md) (various list structures)
  - [PlanRowMark](../P/PlanRowMark.md) (row marking)
- Called from (representative examples):
  - [create_modifytable_path](../c/create_modifytable_path.md) (pathnode.c:3737)
  - [create_modifytable_plan](../c/create_modifytable_plan.md) (createplan.c:2815)
  - [create_plan_recurse](../c/create_plan_recurse.md) (createplan.c:534)

## Notes and Other Information
- The path is always marked as not parallel-safe since data modification operations require coordination that conflicts with parallel execution
- Cost calculation uses the subpath's costs without adding extra overhead, as ModifyTable is typically a top-level node
- Row count is set to subpath rows only if RETURNING clauses are present, otherwise set to 0
- Supports complex inheritance and partitioning scenarios with multiple target relations
- MERGE operations require additional action lists and join conditions for proper execution planning
- [OnConflictExpr](../O/OnConflictExpr.md) analysis is deliberately deferred to execution planning phase for performance reasons