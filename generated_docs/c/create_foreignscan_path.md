# create_foreignscan_path

## Location
[src/backend/optimizer/util/pathnode.c:2235-2280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2235-L2280)

## Overview
Creates a path node for scanning a foreign base table through PostgreSQL's Foreign Data Wrapper (FDW) interface, allowing access to external data sources.

## Definition

```c
ForeignPath *
create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
						PathTarget *target,
						double rows, Cost startup_cost, Cost total_cost,
						List *pathkeys,
						Relids required_outer,
						Path *fdw_outerpath,
						List *fdw_restrictinfo,
						List *fdw_private)
```
## Detailed Description
This function constructs a ForeignPath node specifically for foreign table scan operations. Unlike other path creation functions in PostgreSQL core, this function is never called directly by core PostgreSQL code. Instead, it's designed to be called by Foreign Data Wrapper (FDW) implementations through their GetForeignPaths function. The FDW must supply all cost and row estimation fields since PostgreSQL core has no way to calculate these values for external data sources. The function creates a specialized ForeignPath structure that extends the basic Path structure with FDW-specific fields for storing optimizer state and private data.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global information about the query being planned
- `*rel`: RelOptInfo structure representing the foreign table relation being scanned
- `*target`: PathTarget specifying the desired output columns and expressions (NULL defaults to rel->reltarget)
- `rows`: Estimated number of rows this path will return
- `startup_cost`: Estimated cost to begin returning tuples
- `total_cost`: Estimated total cost to return all tuples
- `*pathkeys`: List of PathKey structures specifying the output ordering
- `required_outer`: Set of relation IDs that must be available as outer relations
- `*fdw_outerpath`: Optional outer path for join pushdown scenarios
- `*fdw_restrictinfo`: List of restriction clauses that can be handled by the FDW
- `*fdw_private`: FDW-specific private data for storing implementation details
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - IS_SIMPLE_REL
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
- Called from (representative examples):
  - Foreign Data Wrapper implementations (external to core PostgreSQL)

## Notes and Other Information
- Returns a ForeignPath structure, not a basic Path structure
- Sets pathtype to T_ForeignScan to identify this as a foreign scan path
- Includes an assertion that the relation must be a simple relation (IS_SIMPLE_REL)
- The FDW must provide all cost estimates since core PostgreSQL cannot calculate them
- Supports the pathtarget defaulting to rel->reltarget when target parameter is NULL
- [Path](../P/Path.md) is marked as not parallel-aware but respects the relation's parallel safety settings
- Essential for PostgreSQL's extensibility through the FDW interface
- The fdw_private field allows FDWs to store implementation-specific optimization data

## Simplified Source

```c
ForeignPath *create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
                                     PathTarget *target,
                                     double rows, Cost startup_cost, Cost total_cost,
                                     List *pathkeys, Relids required_outer,
                                     Path *fdw_outerpath, List *fdw_restrictinfo,
                                     List *fdw_private) {
    // Create new ForeignPath node - used only by FDW implementations
    ForeignPath *pathnode = makeNode(ForeignPath);
    Assert(IS_SIMPLE_REL(rel));

    // Initialize standard path fields
    pathnode->path.pathtype = T_ForeignScan;
    pathnode->path.parent = rel;
    pathnode->path.pathtarget = target ? target : rel->reltarget;
    pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    pathnode->path.parallel_aware = false;
    pathnode->path.parallel_safe = rel->consider_parallel;
    pathnode->path.parallel_workers = 0;

    // Set FDW-provided cost and row estimates
    pathnode->path.rows = rows;
    pathnode->path.startup_cost = startup_cost;
    pathnode->path.total_cost = total_cost;
    pathnode->path.pathkeys = pathkeys;

    // Store FDW-specific information
    pathnode->fdw_outerpath = fdw_outerpath;
    pathnode->fdw_restrictinfo = fdw_restrictinfo;
    pathnode->fdw_private = fdw_private;

    return pathnode;
}
```