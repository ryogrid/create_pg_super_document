# GroupingSetData

## Location
[src/include/nodes/pathnodes.h:2269-2276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2269-L2276)

## Overview
GroupingSetData represents annotations for individual grouping sets in the planner, containing the set definition and cardinality estimates for SQL grouping sets operations.

## Definition
```c
typedef struct GroupingSetData
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;
    List       *set;            /* grouping set as list of sortgrouprefs */
    Cardinality numGroups;      /* est. number of result groups */
} GroupingSetData;
```

## Detailed Description
GroupingSetData is a supporting structure used during query planning to represent individual grouping sets within SQL GROUPING SETS, ROLLUP, and CUBE operations. It serves as an annotation that captures the essential properties of each grouping set for planning purposes.

Each GroupingSetData node represents one specific grouping set, which is a list of columns that should be grouped together in one pass of the grouping operation. The structure stores references to the grouping columns and includes cardinality estimates that are crucial for cost-based optimization decisions.

This structure is primarily used during the preprocessing phase of grouping sets planning, where complex grouping operations are analyzed and broken down into manageable components for execution planning.

## Parameters / Member Variables
- `type`: NodeTag for node type identification (standard PostgreSQL node header)
- `set`: List of sortgroupref integers identifying the columns that make up this grouping set
- `numGroups`: Estimated cardinality (number of distinct groups) expected from this particular grouping set

## Dependencies
- Functions called/Symbols referenced:
  - List (for storing sortgroupref list)
  - NodeTag (node identification)
  - Cardinality (row count estimation type)
  - pg_node_attr (node attributes for copy/equal/read/jumble operations)
- Called from (representative examples):
  - preprocess_grouping_sets (creates and processes GroupingSetData nodes)
  - reorder_grouping_sets (reorders and annotates grouping sets)
  - consider_groupingsets_paths (uses grouping set data for path planning)
  - get_number_of_groups (accesses numGroups estimates)

## Notes and Other Information
- Part of the complex grouping sets infrastructure supporting ROLLUP, CUBE, and GROUPING SETS
- sortgrouprefs are integer references to target list entries that define the grouping columns
- numGroups estimation is filled in during later planning phases for cost calculations
- The pg_node_attr annotation indicates this structure has special handling for copying, equality, reading, and query jumbling
- Used internally by the planner and not directly exposed in execution nodes
- Essential for planning multi-pass aggregation strategies in complex grouping operations
- Helps determine whether grouping sets can be computed efficiently using sorting vs. hashing strategies