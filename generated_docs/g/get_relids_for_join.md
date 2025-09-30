# get_relids_for_join

## Location
[src/backend/optimizer/prep/prepjointree.c:4142-4158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4142-L4158)

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
  - [find_jointree_node_for_rel](../f/find_jointree_node_for_rel.md)
  - [get_relids_in_jointree](get_relids_in_jointree.md)
- Called from (representative examples):
  - [add_nullingrels_if_needed](../a/add_nullingrels_if_needed.md)
  - [alias_relid_set](../a/alias_relid_set.md)

## Notes and Other Information
- This function specifically excludes inner joins (include_inner_joins = false) from the result, following standard planner conventions
- Outer joins are included (include_outer_joins = true) as they are typically part of standard relid sets
- The function will terminate the process with an ERROR if the joinrelid cannot be found, indicating a programming error or corrupted query structure
- This is a higher-level interface that abstracts the complexity of join tree traversal for callers who need to work with specific joins

## Simplified Source

```c
Relids get_relids_for_join(Query *query, int joinrelid) {
    Node *jtnode;

    // Find the join tree node for the specified join relation ID
    jtnode = find_jointree_node_for_rel((Node *) query->jointree, joinrelid);
    if (!jtnode)
        elog(ERROR, "could not find join node %d", joinrelid);

    // Extract all base and outer join relids from the join tree node
    return get_relids_in_jointree(jtnode, true, false);
}
```