# create_hashjoin_path

## Location
src/backend/optimizer/util/pathnode.c: 2619 - 2684

## Overview
Creates a pathnode corresponding to a hash join between two relations, configuring the necessary metadata for hash-based join execution in PostgreSQL's query optimizer.

## Definition
```c
HashPath *create_hashjoin_path(PlannerInfo *root,
                              RelOptInfo *joinrel,
                              JoinType jointype,
                              JoinCostWorkspace *workspace,
                              JoinPathExtraData *extra,
                              Path *outer_path,
                              Path *inner_path,
                              bool parallel_hash,
                              List *restrict_clauses,
                              Relids required_outer,
                              List *hashclauses)
```

## Detailed Description
This function constructs a HashPath node representing a hash join execution plan. Hash joins build a hash table from the inner relation and probe it with rows from the outer relation. The function handles both regular and parallel hash joins based on the parallel_hash parameter. Unlike merge joins, hash joins do not preserve input ordering, so pathkeys are always set to NIL. The function delegates final cost calculation to final_cost_hashjoin after setting up the basic path structure.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `joinrel`: RelOptInfo representing the result relation of the join
- `jointype`: Type of join operation (inner, left outer, etc.)
- `workspace`: Pre-computed cost workspace from initial_cost_hashjoin
- `extra`: Additional join-specific information and flags
- `outer_path`: Path representing the outer (probing) input relation
- `inner_path`: Path representing the inner (hash table) input relation
- `parallel_hash`: Whether to use parallel hash table construction
- `restrict_clauses`: List of RestrictInfo nodes for join conditions
- `required_outer`: Set of outer relations required for parameterized plans
- `hashclauses`: Subset of restrict_clauses used as hash conditions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - get_joinrel_parampathinfo
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)
- Called from (representative examples):
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [try_partial_hashjoin_path](../t/try_partial_hashjoin_path.md)

## Notes and Other Information
Hash joins never have pathkeys since output ordering is unpredictable due to possible batching. The code includes extensive comments about potential optimizations for small inner relations that could preserve outer relation ordering, but these are not currently implemented due to risks of bad size estimates. The num_batches field is filled in later by final_cost_hashjoin.