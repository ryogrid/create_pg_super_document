# set_append_rel_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:1232-1301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L1232-L1301)

## Overview
Builds access paths for an "append relation" by generating paths for each member relation and combining them into the parent relation's pathlist.

## Definition
```c
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for setting up the pathlist for append relations, which are used to represent partitioned tables, inheritance hierarchies, and UNION ALL queries. The function iterates through all child relations of the append relation, generates access paths for each non-dummy child, and then combines these paths into the parent relation using `add_paths_to_append_rel()`.

The function handles parallel safety propagation by ensuring that if the parent append relation is marked as parallel-unsafe, this property is propagated down to all child relations to avoid generating unnecessary partial paths.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context information
- `rel`: RelOptInfo structure representing the append relation for which paths are being generated
- `rti`: Range table index of the append relation in the query's range table
- `rte`: RangeTblEntry corresponding to the append relation in the range table

## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (struct type for append relation information)
  - [set_rel_pathlist](set_rel_pathlist.md) (generates paths for individual child relations)
  - IS_DUMMY_REL (macro to check if a relation is dummy/empty)
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md) (combines child paths into parent append paths)
- Called from (representative examples):
  - [set_rel_pathlist](set_rel_pathlist.md) (main pathlist generation dispatcher)

## Notes and Other Information
- The function only processes child relations that belong to the current parent by checking `appinfo->parent_relid != parentRTindex`
- Dummy (empty) child relations are excluded from path generation to avoid unnecessary overhead
- Parallel safety is propagated from parent to children to ensure consistent parallel execution planning
- The live_childrels list maintains only non-dummy children for efficient path generation
- This function is part of the PostgreSQL query optimizer's path generation phase