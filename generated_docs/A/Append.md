# Append

## Location
src/include/nodes/plannodes.h: 265 - 280

## Overview
The Append node generates the concatenation of results from multiple sub-plans, used primarily in UNION operations and partitioned table queries.

## Definition


## Detailed Description
The Append execution node is responsible for concatenating results from multiple child plans in sequence. It is commonly used in UNION queries where results from different SELECT statements need to be combined, and in partitioned table access where multiple partitions are queried. The node executes each child plan in turn and returns their results as a unified stream.

The Append node supports both synchronous and asynchronous execution modes, allowing for parallel execution of some child plans when beneficial. It also includes runtime partition pruning capabilities to skip unnecessary partitions based on query conditions.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information
- : Bitmapset containing Range Table Indexes (RTIs) of append relations formed by this node
- : List of child Plan nodes whose results will be concatenated
- : Number of child plans that can be executed asynchronously for parallel processing
- : Index separating non-partial plans (before this index) from partial plans (from this index onwards)
- : Pointer to PartitionPruneInfo structure for runtime subplan pruning, NULL if pruning is not used

## Dependencies
- Functions called/Symbols referenced:
  - PartitionPruneInfo
- Called from (representative examples):
  - ExecInitAppend
  - create_append_plan
  - set_append_references
  - ExplainNode

## Notes and Other Information
- The Append node is fundamental to PostgreSQL's execution of UNION operations and partitioned table queries
- Supports runtime partition pruning to improve performance by skipping unnecessary partitions
- Can execute child plans asynchronously for better parallelism
- The distinction between partial and non-partial plans enables hybrid execution strategies in parallel query processing
- Located in src/include/nodes/plannodes.h:265-280