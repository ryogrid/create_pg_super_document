# get_relids_for_join

## Location
src/backend/optimizer/prep/prepjointree.c: 4142 - 4158

## Overview
Retrieves the set of base relation and outer join RT indexes that make up a specific join by its relation ID.

## Definition
```c
Relids get_relids_for_join(Query *query, int joinrelid)
```

## Detailed Description
This function serves as a convenience wrapper that combines two operations: locating a specific join node within the query's join tree and extracting all the base relation and outer join RT indexes that comprise that join.

The function first uses `find_jointree_node_for_rel` to locate the join tree node corresponding to the given `joinrelid`. Once found, it calls `get_relids_in_jointree` with parameters set to include outer joins but exclude inner joins (true, false), which is the standard configuration for most planner operations.

If the specified join relation ID cannot be found in the join tree, the function raises an ERROR, indicating a serious internal inconsistency.

## Parameters / Member Variables
- `query`: The Query structure containing the join tree to search
- `joinrelid`: The range table index of the join relation to find and analyze

## Dependencies
- Functions called/Symbols referenced:
  - find_jointree_node_for_rel
  - get_relids_in_jointree
- Called from (representative examples):
  - add_nullingrels_if_needed
  - alias_relid_set

## Notes and Other Information
- This function specifically excludes inner joins (include_inner_joins = false) from the result, following standard planner conventions
- Outer joins are included (include_outer_joins = true) as they are typically part of standard relid sets
- The function will terminate the process with an ERROR if the joinrelid cannot be found, indicating a programming error or corrupted query structure
- This is a higher-level interface that abstracts the complexity of join tree traversal for callers who need to work with specific joins