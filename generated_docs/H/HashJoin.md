# HashJoin

## Location
[src/include/nodes/plannodes.h:862-874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L862-L874)

## Overview
HashJoin is a plan node that implements the hash join algorithm, which builds a hash table from the inner relation and probes it with tuples from the outer relation to find matches.

## Definition

```c
typedef struct HashJoin
{
	Join		join;
	List	   *hashclauses;
	List	   *hashoperators;
	List	   *hashcollations;

	/*
	 * List of expressions to be hashed for tuples from the outer plan, to
	 * perform lookups in the hashtable over the inner plan.
	 */
	List	   *hashkeys;
} HashJoin;
```
## Detailed Description
HashJoin implements the hash join algorithm, which is typically the most efficient join method for equijoin conditions when one of the relations is significantly smaller than the other. The algorithm operates in two phases: build phase and probe phase.

During the build phase, PostgreSQL constructs a hash table from the inner relation (typically the smaller one), using the join keys as hash keys. During the probe phase, it scans the outer relation, hashes each tuple's join key values, and looks up matching tuples in the hash table.

The hashclauses contain the join equality conditions, while hashoperators and hashcollations specify how to perform the equality comparisons and hashing. The hashkeys list contains expressions that will be evaluated and hashed for outer relation tuples to perform hash table lookups.

Hash joins are particularly effective for large datasets where neither relation is pre-sorted, as they have O(M+N) time complexity and can handle very large datasets efficiently, though they require sufficient memory to hold the hash table.

## Parameters / Member Variables
- `join`: Base Join structure containing common join information (plan details, join type, join qualifiers)
- `*hashclauses`: List of join equality clauses that will be evaluated using hash-based matching
- `*hashoperators`: List of equality operator OIDs corresponding to each hash clause for performing tuple comparisons
- `*hashcollations`: List of collation OIDs to use when hashing and comparing values for each hash clause
- `*hashkeys`: List of expressions to be evaluated and hashed for outer relation tuples when probing the hash table
## Dependencies
- Functions called/Symbols referenced:
  - [Join](../J/Join.md) (inherited base structure)
  - [List](../L/List.md) (for various clause and key lists)

- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (commands/explain.c:2188)
  - [ExecInitNode](../E/ExecInitNode.md) (executor/execProcnode.c:308)
  - [ExecInitHashJoin](../E/ExecInitHashJoin.md) (executor/nodeHashjoin.c:710)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md) (optimizer/plan/createplan.c:4750)
  - [make_hashjoin](../m/make_hashjoin.md) (optimizer/plan/createplan.c:5986)

## Notes and Other Information
- [Hash](Hash.md) joins require equality conditions and cannot handle inequality joins
- The inner relation is chosen as the smaller relation to minimize memory usage for the hash table
- If the hash table doesn't fit in work_mem, PostgreSQL uses a batching strategy to process the join in multiple passes
- [Hash](Hash.md) joins are often preferred by the query planner for large tables without useful indexes
- The algorithm handles multiple join keys by creating composite hash values
- Memory requirements can be significant for large inner relations, potentially affecting performance if spilling to disk occurs
- [Hash](Hash.md) collisions are handled through chaining in the hash table implementation