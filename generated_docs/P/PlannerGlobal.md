# PlannerGlobal

## Location
[src/include/nodes/pathnodes.h:95-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L95-L166)

## Overview
PlannerGlobal holds global state information shared across an entire planner invocation, including all levels of sub-queries within the command being planned.

## Definition
```c
typedef struct PlannerGlobal
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag         type;

    /* Param values provided to planner() */
    ParamListInfo boundParams pg_node_attr(read_write_ignore);

    /* Plans for SubPlan nodes */
    List           *subplans;

    /* Paths from which the SubPlan Plans were made */
    List           *subpaths;

    /* PlannerInfos for SubPlan nodes */
    List           *subroots pg_node_attr(read_write_ignore);

    /* indices of subplans that require REWIND */
    Bitmapset      *rewindPlanIDs;

    /* "flat" rangetable for executor */
    List           *finalrtable;

    /* "flat" list of RTEPermissionInfos */
    List           *finalrteperminfos;

    /* "flat" list of PlanRowMarks */
    List           *finalrowmarks;

    /* "flat" list of integer RT indexes */
    List           *resultRelations;

    /* "flat" list of AppendRelInfos */
    List           *appendRelations;

    /* OIDs of relations the plan depends on */
    List           *relationOids;

    /* other dependencies, as PlanInvalItems */
    List           *invalItems;

    /* type OIDs for PARAM_EXEC Params */
    List           *paramExecTypes;

    /* highest PlaceHolderVar ID assigned */
    Index           lastPHId;

    /* highest PlanRowMark ID assigned */
    Index           lastRowMarkId;

    /* highest plan node ID assigned */
    int             lastPlanNodeId;

    /* redo plan when TransactionXmin changes? */
    bool            transientPlan;

    /* is plan specific to current role? */
    bool            dependsOnRole;

    /* parallel mode potentially OK? */
    bool            parallelModeOK;

    /* parallel mode actually required? */
    bool            parallelModeNeeded;

    /* worst PROPARALLEL hazard level */
    char            maxParallelHazard;

    /* partition descriptors */
    PartitionDirectory partition_directory pg_node_attr(read_write_ignore);
} PlannerGlobal;
```

## Detailed Description
PlannerGlobal serves as the central repository for state information that must be shared across all planning phases and sub-query levels during query optimization. This structure maintains global context for an entire planner invocation, ensuring consistency and coordination between different parts of the planning process. It tracks subplans and their associated information, manages identifier assignment, maintains flattened data structures for the executor, and coordinates parallel execution decisions. The structure is designed to accumulate information as planning progresses and provide the executor with all necessary metadata in easily accessible formats.

## Parameters / Member Variables
- `type`: Standard PostgreSQL node type tag for identification
- `boundParams`: Parameter values provided to the planner for parameterized queries
- `subplans`: List of Plan nodes for SubPlan expressions (subqueries)
- `subpaths`: List of Path nodes from which the SubPlan Plans were generated
- `subroots`: List of PlannerInfo structures for SubPlan nodes
- `rewindPlanIDs`: Bitmap identifying subplans that require REWIND capability
- `finalrtable`: Flattened range table for executor use
- `finalrteperminfos`: Flattened list of range table entry permission information
- `finalrowmarks`: Flattened list of PlanRowMark structures
- `resultRelations`: Flattened list of result relation RT indexes
- `appendRelations`: Flattened list of AppendRelInfo structures for inheritance
- `relationOids`: OIDs of relations the plan depends on for cache invalidation
- `invalItems`: Other plan dependencies as PlanInvalItem structures
- `paramExecTypes`: Type OIDs for PARAM_EXEC parameters
- `lastPHId`: Counter for assigning unique PlaceHolderVar IDs
- `lastRowMarkId`: Counter for assigning unique PlanRowMark IDs
- `lastPlanNodeId`: Counter for assigning unique plan node IDs
- `transientPlan`: Flag indicating plan needs regeneration when TransactionXmin changes
- `dependsOnRole`: Flag indicating plan is specific to current user role
- `parallelModeOK`: Flag indicating parallel execution is potentially acceptable
- `parallelModeNeeded`: Flag indicating parallel execution is actually required
- `maxParallelHazard`: Worst parallel hazard level encountered in the plan
- `partition_directory`: Directory of partition descriptors for partitioned tables

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo (for boundParams)
  - PartitionDirectory (for partition_directory)
  - NodeTag, List, Bitmapset, Index (standard PostgreSQL types)
- Called from (representative examples):
  - standard_planner (main planner entry point)
  - subquery_planner (subquery planning)
  - expression_planner_with_deps (expression planning)
  - set_plan_references (plan reference fixing)

## Notes and Other Information
- Central coordination point for all global planner state, ensuring consistency across complex multi-level planning operations
- The "flattened" data structures prepare information in the format expected by the executor
- ID assignment counters ensure uniqueness across the entire plan tree
- Parallel execution flags coordinate parallel safety decisions across all plan components
- Cache invalidation information enables proper plan cache management
- Essential for handling complex queries with multiple subquery levels and inheritance hierarchies