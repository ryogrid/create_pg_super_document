# create_tidscan_path

## Location
[src/backend/optimizer/util/pathnode.c:1179-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1179-L1207)

## Overview
Creates a path node corresponding to a scan by TID (Tuple Identifier), which allows direct access to specific tuples when their physical locations are known.

## Definition
```c
TidPath *create_tidscan_path(PlannerInfo *root, 
                             RelOptInfo *rel, 
                             List *tidquals,
                             Relids required_outer)
```

## Detailed Description
This function constructs a TidPath node that represents a TID scan access path. A TID scan is a specialized scan method that directly accesses tuples using their tuple identifiers (TIDs), which are physical addresses consisting of a block number and tuple offset within that block. This scan method is typically used when queries contain conditions like "WHERE ctid = '(0,1)'" or when the planner determines that direct TID access is more efficient than other scan methods.

The function initializes the standard Path structure fields and calls cost_tidscan to estimate execution costs. TID scans are not parallel-aware since they typically access a small, specific set of tuples, and the resulting path is always unordered since TIDs represent physical locations rather than logical ordering.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and optimization information
- `rel`: RelOptInfo for the relation being scanned, containing statistics and metadata
- `tidquals`: List of TID qualification expressions that specify which TIDs to scan
- `required_outer`: Set of outer relation IDs needed for a parameterized path (for joins)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new TidPath node)
  - get_baserel_parampathinfo (gets parameterization info for the path)
  - [cost_tidscan](cost_tidscan.md) (calculates startup and total execution costs)
- Called from (representative examples):
  - [BuildParameterizedTidPaths](../B/BuildParameterizedTidPaths.md) (for parameterized TID scan paths)
  - [create_tidscan_paths](create_tidscan_paths.md) (when generating TID scan alternatives)

## Notes and Other Information
- The resulting path is always unordered (pathkeys = NIL) since TID scans access tuples by physical location
- Not parallel-aware but inherits parallel safety characteristics from the relation
- Typically used for queries with explicit CTID conditions or when the planner determines direct tuple access is optimal
- TID scans are generally very fast for accessing small numbers of specific tuples
- The tidquals list contains expressions that evaluate to TID values, often involving the CTID system column
- Cost estimation considers the number of TIDs to fetch and the random I/O pattern typical of TID access