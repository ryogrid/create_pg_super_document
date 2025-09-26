# HashPath

## Location
[src/include/nodes/pathnodes.h:2151-2157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2151-L2157)

## Overview
HashPath represents a hash join execution path in PostgreSQL's query planner, storing the necessary information to execute a hash join operation between two relations.

## Definition

```c
typedef struct HashPath
{
	JoinPath	jpath;
	List	   *path_hashclauses;	/* join clauses used for hashing */
	int			num_batches;	/* number of batches expected */
	Cardinality inner_rows_total;	/* total inner rows expected */
} HashPath;
```
## Detailed Description
HashPath is a specialized path node that extends JoinPath to represent hash join operations. Hash joins work by building a hash table from the smaller (inner) relation and then probing it with tuples from the larger (outer) relation. Unlike merge joins, hash joins do not require any particular ordering of their inputs, making them more flexible for unordered data.

The planner creates HashPath nodes when evaluating different join strategies, and these paths compete with other join methods (nested loop, merge join) based on cost estimates. The hash join implementation can handle cases where the inner relation is too large to fit entirely in memory by using batching - dividing both relations into corresponding batches that can be processed sequentially.

## Parameters / Member Variables
- `jpath`: The base JoinPath structure containing common join path information (join type, outer/inner paths, join conditions, etc.)
- `*path_hashclauses`: List of join clauses that will be used for hash table construction and probing - these are the equality conditions suitable for hashing
- `num_batches`: Expected number of batches needed if the hash table doesn't fit in available memory (1 means no batching required)
- `inner_rows_total`: Total estimated cardinality of the inner relation, used for cost calculations and memory allocation decisions
## Dependencies
- Functions called/Symbols referenced:
  - [JoinPath](../J/JoinPath.md) (inherited base structure)
  - Cardinality (type for row count estimates)
- Called from (representative examples):
  - [create_hashjoin_path](../c/create_hashjoin_path.md) (path creation)
  - [create_hashjoin_plan](../c/create_hashjoin_plan.md) (plan generation)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md) (cost calculation)
  - [cost_rescan](../c/cost_rescan.md) (rescan cost estimation)

## Notes and Other Information
- [HashPath](HashPath.md) inherits from JoinPath, which provides the basic join path infrastructure including outer/inner paths and join conditions
- [Hash](Hash.md) joins are generally efficient for equi-joins and are often preferred when neither input is already sorted on the join keys
- The batching mechanism allows hash joins to handle datasets larger than available memory by processing data in chunks
- Unlike MergePath, HashPath doesn't need to track sort keys since hash joins don't require sorted inputs
- The path_hashclauses specifically contain equality conditions suitable for hashing, which may be a subset of the total join conditions