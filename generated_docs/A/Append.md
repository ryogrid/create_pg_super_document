# Append

## Location
[src/include/nodes/plannodes.h:265-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L265-L280)

## Overview
The Append node generates the concatenation of results from multiple sub-plans, used primarily in UNION operations and partitioned table queries.

## Definition

```c
typedef struct Append
{
	Plan		plan;
	Bitmapset  *apprelids;		/* RTIs of appendrel(s) formed by this node */
	List	   *appendplans;
	int			nasyncplans;	/* # of asynchronous plans */

	/*
	 * All 'appendplans' preceding this index are non-partial plans. All
	 * 'appendplans' from this index onwards are partial plans.
	 */
	int			first_partial_plan;

	/* Info for run-time subplan pruning; NULL if we're not doing that */
	struct PartitionPruneInfo *part_prune_info;
} Append;
```
## Detailed Description
The Append execution node is responsible for concatenating results from multiple child plans in sequence. It is commonly used in UNION queries where results from different SELECT statements need to be combined, and in partitioned table access where multiple partitions are queried. The node executes each child plan in turn and returns their results as a unified stream.

The Append node supports both synchronous and asynchronous execution modes, allowing for parallel execution of some child plans when beneficial. It also includes runtime partition pruning capabilities to skip unnecessary partitions based on query conditions.

## Parameters / Member Variables
- `plan`: Base Plan structure containing common plan node information
- `*apprelids`: Bitmapset containing Range Table Indexes (RTIs) of append relations formed by this node
- `*appendplans`: List of child Plan nodes whose results will be concatenated
- `nasyncplans`: Number of child plans that can be executed asynchronously for parallel processing
- `first_partial_plan`: Index separating non-partial plans (before this index) from partial plans (from this index onwards)
- `*part_prune_info`: Pointer to PartitionPruneInfo structure for runtime subplan pruning, NULL if pruning is not used
## Dependencies
- Functions called/Symbols referenced:
  - [PartitionPruneInfo](../P/PartitionPruneInfo.md)
- Called from (representative examples):
  - [ExecInitAppend](../E/ExecInitAppend.md)
  - [create_append_plan](../c/create_append_plan.md)
  - [set_append_references](../s/set_append_references.md)
  - [ExplainNode](../E/ExplainNode.md)

## Notes and Other Information
- The Append node is fundamental to PostgreSQL's execution of UNION operations and partitioned table queries
- Supports runtime partition pruning to improve performance by skipping unnecessary partitions
- Can execute child plans asynchronously for better parallelism
- The distinction between partial and non-partial plans enables hybrid execution strategies in parallel query processing
- Located in src/include/nodes/plannodes.h:265-280