# create_valuesscan_path

## Location
[src/backend/optimizer/util/pathnode.c:2098-2123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2098-L2123)

## Overview
Creates a Path node corresponding to a scan of a VALUES list, which represents accessing data from a VALUES clause that provides explicit row data in PostgreSQL's query planner.

## Definition
```c
Path *create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
                            Relids required_outer)
```

## Detailed Description
The create_valuesscan_path function constructs a basic Path node that represents scanning a VALUES list. VALUES clauses provide explicit row data directly in the query (e.g., VALUES (1, 'a'), (2, 'b')) and are treated as a special type of relation in the planner. This function creates the path representation for accessing this literal data.

Key behaviors include:
- Creates a basic Path node (not a specialized subtype) for VALUES scanning
- Always produces unordered results (pathkeys = NIL) since VALUES lists have no inherent ordering
- Sets parallel safety based on the relation's consider_parallel flag
- Does not support parallel workers (parallel_workers = 0) as VALUES data is typically small
- Uses the relation's target list as the output specification
- Delegates cost calculation to cost_valuesscan for accurate estimates

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information and context
- `rel`: RelOptInfo structure representing the VALUES relation being scanned
- `required_outer`: Relids indicating which outer relations are required for parameter passing

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Path node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (to get parameter information)
  - [cost_valuesscan](cost_valuesscan.md) (to calculate execution costs)

- Called from (representative examples):
  - [set_values_pathlist](../s/set_values_pathlist.md) (in allpaths.c:2828)

## Notes and Other Information
- Returns a basic Path node rather than a specialized path type, indicating VALUES scans are straightforward operations
- VALUES scans always produce unordered results since the VALUES clause doesn't guarantee any specific ordering
- Not parallel-aware and does not use parallel workers, as VALUES data is typically small and in-memory
- The parallel_safe property depends on the relation's consider_parallel setting, though VALUES are generally safe
- Commonly used for explicit data like INSERT ... VALUES, VALUES clauses in FROM, and Common Table Expressions (CTEs)
- Cost calculation considers the number of rows and columns in the VALUES list, as well as expression evaluation costs
- VALUES data is materialized at execution time from the literal values specified in the query