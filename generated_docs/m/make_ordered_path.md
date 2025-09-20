# make_ordered_path

## Location
[src/backend/optimizer/plan/planner.c:6993-7043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6993-L7043)

## Overview
Creates an ordered path from a given input path by adding sort operations when necessary to satisfy specified pathkey requirements.

## Definition

```c
static Path *
make_ordered_path(PlannerInfo *root, RelOptInfo *rel, Path *path,
				  Path *cheapest_path, List *pathkeys)
```
## Detailed Description
The  function creates a path that produces results in a specific order defined by . It intelligently chooses between different sorting strategies based on the input path's existing ordering:

1. **Pre-sorted analysis**: Uses  to determine if the input path already satisfies the required ordering and counts pre-sorted keys
2. **Path selection logic**: Only processes paths worth considering - either the cheapest path or paths with some pre-existing order when incremental sort is enabled
3. **Sort strategy selection**:
   - **Full sort**: When no keys are pre-sorted or incremental sort is disabled
   - **Incremental sort**: When some keys are already sorted and incremental sort is enabled

The function optimizes performance by avoiding unnecessary sort operations when the path is already appropriately ordered and by choosing the most efficient sorting method based on existing ordering.

## Parameters / Member Variables
- : PlannerInfo containing planner context and configuration
- : RelOptInfo for the relation being planned
- : Input path that may need ordering
- : Reference to the cheapest available path for comparison
- : List of pathkeys specifying the desired ordering

## Dependencies
- Functions called/Symbols referenced:
  -  - Analyzes path ordering compatibility
  -  - Creates a full sort path
  -  - Creates an incremental sort path for partially ordered input
- Called from (representative examples):
  -  - When creating grouped paths that need ordering
  -  - During partial grouping path creation

## Notes and Other Information
- Returns NULL when creating an ordered path doesn't make sense (non-cheapest path with no pre-sorting)
- Static function - only used within the planner module
- Respects  configuration setting
- Incremental sort is preferred when applicable as it's more efficient for partially sorted data
- Used primarily in grouping and aggregation planning where result ordering matters
- Avoids creating redundant paths by checking if the input is already sufficiently ordered
- The function balances between path cost and sorting requirements to find optimal solutions