# make_hashjoin

## Location
[src/backend/optimizer/plan/createplan.c:5974-6004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5974-L6004)

## Overview
Creates a HashJoin plan node that represents a hash join operation in PostgreSQL's query execution plan tree.

## Definition
```c
static HashJoin *make_hashjoin(List *tlist, List *joinclauses, List *otherclauses, List *hashclauses, List *hashoperators, List *hashcollations, List *hashkeys, Plan *lefttree, Plan *righttree, JoinType jointype, bool inner_unique)
```

## Detailed Description
The `make_hashjoin` function constructs a HashJoin plan node, which implements the hash join algorithm. Hash joins work by building a hash table from the inner relation (typically the smaller one) and then probing this hash table for each row of the outer relation. This join method is particularly efficient for large datasets and is often chosen when there are no suitable indexes and the join conditions are equality-based.

The function initializes all the hash-specific fields including hash clauses, operators, collations, and keys that define how the hash table will be built and probed. This join algorithm typically has O(M+N) complexity, making it very efficient for joining large relations.

## Parameters / Member Variables
- `tlist`: Target list specifying the columns to be output by this join node
- `joinclauses`: List of join qualification clauses that determine matching conditions between relations
- `otherclauses`: List of other qualification clauses (non-join conditions) to be applied at this node
- `hashclauses`: List of clauses that will be used for hashing (typically equality conditions)
- `hashoperators`: List of operators used for hash key comparison
- `hashcollations`: List of collations for the hash keys (important for string comparisons)
- `hashkeys`: List of expressions from the outer relation that will be used as hash keys
- `lefttree`: Plan node representing the outer relation
- `righttree`: Plan node representing the inner relation (will be hashed)
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `inner_unique`: Boolean indicating whether the inner relation produces at most one matching row for each outer row

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the HashJoin node)
  - [HashJoin](../H/HashJoin.md) (plan node structure)
  - JoinType (enumeration for join types)
- Called from (representative examples):
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md) (in createplan.c:4902)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the plan creation subsystem
- [Hash](../H/Hash.md) joins are typically chosen when both relations are large and no suitable indexes exist
- The hash table is built from the inner relation (righttree), so the planner usually assigns the smaller relation as the inner relation
- [Hash](../H/Hash.md) joins require equality conditions in the join clauses to work effectively
- The hash-specific parameters (hashclauses, hashoperators, etc.) are crucial for the hash table construction and probing phases
- Memory usage for the hash table is a key consideration - if it doesn't fit in work_mem, PostgreSQL may spill to disk or choose a different join algorithm