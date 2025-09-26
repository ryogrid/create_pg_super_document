# create_subqueryscan_path

## Location
[src/backend/optimizer/util/pathnode.c:2016-2045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2016-L2045)

## Overview
Creates a SubqueryScanPath node corresponding to a scan of a subquery, which represents accessing the results of a nested query as if it were a regular table in PostgreSQL's query planner.

## Definition
```c
SubqueryScanPath *create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                                          bool trivial_pathtarget,
                                          List *pathkeys, Relids required_outer)
```

## Detailed Description
The create_subqueryscan_path function constructs a SubqueryScanPath node that represents scanning the results of a subquery. This is used when a subquery appears in the FROM clause and needs to be treated as a data source. The function sets up the path properties including parallel execution capabilities inherited from the subpath, cost calculations, and output ordering.

Key behaviors include:
- Inherits parallel safety from the underlying subpath and relation's parallel consideration
- Uses the relation's target list (reltarget) as the path target
- Preserves the parallel worker count from the subpath
- Delegates cost calculation to cost_subqueryscan with optimization for trivial targets
- Maintains the specified pathkeys for output ordering

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information and context
- `rel`: RelOptInfo structure representing the subquery relation being scanned
- `subpath`: The path representing the execution plan for the subquery itself
- `trivial_pathtarget`: Boolean indicating if rel->reltarget is trivial (just fetching all subquery columns in order)
- `pathkeys`: List specifying the desired output ordering for the scan results
- `required_outer`: Relids indicating which outer relations are required for parameter passing

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create SubqueryScanPath node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (to get parameter information)
  - [cost_subqueryscan](cost_subqueryscan.md) (to calculate execution costs)
  - [SubqueryScanPath](../S/SubqueryScanPath.md) (the path node structure)

- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md) (in allpaths.c:2710, 2736)
  - [build_setop_child_paths](../b/build_setop_child_paths.md) (in prepunion.c:557, 625, 651)
  - [reparameterize_path](../r/reparameterize_path.md) (in pathnode.c:4009)

## Notes and Other Information
- The trivial_pathtarget parameter is an optimization hint that the caller can provide to improve cost calculation efficiency
- Parallel safety is determined by both the subpath's parallel safety and the relation's consider_parallel flag
- The function preserves the pathkeys from the caller, allowing for ordered subquery scans
- Cost calculation considers whether the target is trivial to optimize for simple column projections
- Used extensively in set operations (UNION, INTERSECT, EXCEPT) and general subquery processing