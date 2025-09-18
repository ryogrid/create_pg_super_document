# create_tidrangescan_path

## Location
[src/backend/optimizer/util/pathnode.c:1208-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L1208-L1243)

## Overview
Creates a path node corresponding to a scan by a range of TIDs (Tuple Identifiers), which allows efficient access to tuples within specified TID ranges rather than individual TIDs.

## Definition
```c
TidRangePath *create_tidrangescan_path(PlannerInfo *root, 
                                       RelOptInfo *rel,
                                       List *tidrangequals, 
                                       Relids required_outer)
```

## Detailed Description
This function constructs a TidRangePath node that represents a TID range scan access path. Unlike regular TID scans that access individual tuples by their specific TIDs, a TID range scan processes ranges of TIDs, making it more efficient when scanning contiguous or semi-contiguous blocks of tuples. This scan method is particularly useful for queries with conditions involving TID ranges, such as "WHERE ctid BETWEEN '(0,1)' AND '(0,100)'" or when the planner determines that scanning a range of physical locations is more efficient than individual TID lookups.

The function follows the same initialization pattern as other scan path creators, setting up the standard Path structure and calling the specialized cost estimation function. Like regular TID scans, TID range scans are not parallel-aware and produce unordered results since they operate on physical tuple locations.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and optimization information
- `rel`: RelOptInfo for the relation being scanned, containing statistics and metadata
- `tidrangequals`: List of TID range qualification expressions that specify the TID ranges to scan
- `required_outer`: Set of outer relation IDs needed for a parameterized path (for joins)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new TidRangePath node)
  - get_baserel_parampathinfo (gets parameterization info for the path)
  - [cost_tidrangescan](cost_tidrangescan.md) (calculates startup and total execution costs for range scanning)
- Called from (representative examples):
  - [create_tidscan_paths](create_tidscan_paths.md) (when generating TID-based scan alternatives including range scans)

## Notes and Other Information
- The resulting path is always unordered (pathkeys = NIL) since TID range scans access tuples by physical location
- Not parallel-aware but inherits parallel safety characteristics from the relation
- More efficient than individual TID scans when accessing multiple tuples in nearby physical locations
- Typically used for range queries on the CTID system column or similar TID-based conditions
- The tidrangequals list contains expressions that evaluate to TID ranges, often involving range operators on CTID
- Cost estimation considers the number of blocks that need to be accessed within the specified TID ranges
- Can be significantly more I/O efficient than discrete TID scans when tuples are physically clustered