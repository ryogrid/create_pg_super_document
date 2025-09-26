# RollupData

## Location
[src/include/nodes/pathnodes.h:2278-2289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2278-L2289)

## Overview
RollupData represents a collection of related grouping sets that can be computed together in a single aggregation pass, organizing grouping sets into efficient rollup operations for GROUPING SETS, ROLLUP, and CUBE queries.

## Definition
```c
typedef struct RollupData
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;
    List       *groupClause;    /* applicable subset of parse->groupClause */
    List       *gsets;          /* lists of integer indexes into groupClause */
    List       *gsets_data;     /* list of GroupingSetData */
    Cardinality numGroups;      /* est. number of result groups */
    bool        hashable;       /* can be hashed */
    bool        is_hashed;      /* to be implemented as a hashagg */
} RollupData;
```

## Detailed Description
RollupData is a central planning structure that organizes multiple related grouping sets into efficient execution strategies. It represents a collection of grouping sets that share common prefixes and can be computed together using a single aggregation pass, either through sorted aggregation or hash aggregation.

The structure is created during the preprocessing phase of GROUPING SETS planning, where the planner analyzes the relationships between different grouping sets and groups them into rollups that can be computed efficiently. Each RollupData represents one such group of related sets that can share computation.

RollupData is essential for implementing complex SQL constructs like ROLLUP, CUBE, and GROUPING SETS efficiently, allowing the database to minimize the number of aggregation passes required by leveraging shared computation across related grouping operations.

## Parameters / Member Variables
- `type`: NodeTag for node type identification (standard PostgreSQL node header)
- `groupClause`: List of SortGroupClause structures defining the applicable grouping columns for this rollup
- `gsets`: Lists of integer indexes mapping into the groupClause, representing the specific grouping sets in this rollup
- `gsets_data`: List of GroupingSetData nodes containing detailed information about each grouping set
- `numGroups`: Estimated total number of groups that will be produced by all grouping sets in this rollup
- `hashable`: Boolean flag indicating whether this rollup can be computed using hash aggregation
- `is_hashed`: Boolean flag indicating whether this rollup will actually be implemented using hash aggregation

## Dependencies
- Functions called/Symbols referenced:
  - List (for groupClause, gsets, and gsets_data)
  - GroupingSetData (detailed grouping set information)
  - SortGroupClause (grouping column specifications)
  - NodeTag (node identification)
  - Cardinality (row count estimation type)
  - pg_node_attr (node attributes)
- Called from (representative examples):
  - preprocess_grouping_sets (creates and populates RollupData nodes)
  - consider_groupingsets_paths (uses rollup data for path planning)
  - create_groupingsets_path (incorporates rollup data into execution paths)
  - create_groupingsets_plan (converts rollup data to execution plans)

## Notes and Other Information
- Core component of PostgreSQL's advanced grouping sets implementation
- Enables efficient execution of ROLLUP, CUBE, and GROUPING SETS operations
- The distinction between hashable and is_hashed allows for flexible execution strategy decisions
- gsets contain integer indexes that map to positions in the groupClause for efficient reference
- Multiple RollupData structures may be created for complex grouping operations requiring multiple passes
- Used in both AGG_SORTED and AGG_HASHED aggregation strategies
- Critical for cost estimation in complex aggregation scenarios
- The pg_node_attr annotation provides special handling for node operations