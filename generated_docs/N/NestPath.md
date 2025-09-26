# NestPath

## Location
src/include/nodes/pathnodes.h: 2092 - 2095

## Overview
NestPath represents a nested loop join algorithm path in PostgreSQL query planning, implementing the simplest and most general join method.

## Definition
```c
typedef struct NestPath
{
    JoinPath    jpath;
} NestPath;
```

## Detailed Description
NestPath is a concrete implementation of JoinPath that represents the nested loop join algorithm. As the comment indicates, "A nested-loop path needs no special fields" beyond what is provided by the base JoinPath structure. This reflects the simplicity of the nested loop algorithm: for each tuple from the outer relation, scan the inner relation looking for matching tuples.

The nested loop join is the most fundamental join algorithm in PostgreSQL. It can handle any join condition (not just equality) and works with any data types. While often not the most efficient for large datasets, it is sometimes the only viable option for certain join conditions and is typically optimal when one side of the join is very small or when the inner side can be efficiently accessed via an index.

The simplicity of the NestPath structure belies the sophistication of the nested loop implementation, which can utilize parameterized paths, index scans driven by outer relation values, and various optimization techniques.

## Parameters / Member Variables
- `jpath`: The base JoinPath structure containing all standard join path information including:
  - Base path information (costs, cardinality, ordering)
  - Join type and join conditions
  - Outer and inner join paths
  - Inner uniqueness information

## Dependencies
- Functions called/Symbols referenced:
  - JoinPath (base structure)

- Called from (representative examples):
  - GetExistingLocalJoinPath (foreign data wrapper integration)
  - final_cost_nestloop (cost calculation specific to nested loops)
  - create_nestloop_plan (converts path to execution plan)
  - create_nestloop_path (creates new NestPath instances)
  - calc_non_nestloop_required_outer (parameterization analysis)
  - has_indexed_join_quals (optimization analysis)

## Notes and Other Information
- Nested loop joins are often optimal when the outer relation is small or the inner relation can be efficiently accessed via an index
- Can handle any join condition, not just equijoins, making it more flexible than hash or merge joins
- Performance is typically O(N*M) where N and M are the sizes of outer and inner relations, but can be much better with proper indexing
- Supports parameterized inner paths where the inner scan can use values from the outer tuple to drive index lookups
- The lack of additional fields in NestPath demonstrates that the complexity lies in the algorithm implementation, not the path representation
- Cost estimation considers factors like startup costs, per-tuple costs, and potential for index-driven inner scans
- Often used as a fallback when other join algorithms are not applicable due to join condition constraints