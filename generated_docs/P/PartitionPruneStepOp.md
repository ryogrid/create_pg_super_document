# PartitionPruneStepOp

## Location
[src/include/nodes/plannodes.h:1527-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1527-L1535)

## Overview
PartitionPruneStepOp is a concrete implementation of PartitionPruneStep that contains information for pruning partitions using a set of mutually ANDed OpExpr clauses, representing operator-based partition elimination logic.

## Definition
```c
typedef struct PartitionPruneStepOp
{
    PartitionPruneStep step;

    StrategyNumber opstrategy;
    List       *exprs;
    List       *cmpfns;
    Bitmapset  *nullkeys;
} PartitionPruneStepOp;
```

## Detailed Description
PartitionPruneStepOp extends PartitionPruneStep to handle partition pruning based on operator expressions. It extracts information from up to partnatts OpExpr clauses (where partnatts is the number of partition key columns) to determine which partitions can be eliminated during query planning.

The structure works by building a lookup key from the expressions and using comparison functions to evaluate partition bounds. The opstrategy determines how partitions are selected - for equality operations, only matching partitions are included, while for range operations, appropriate sets of partitions are selected based on the comparison strategy.

For hash partitioning, the nullkeys bitmapset tracks which partition keys were matched to IS NULL clauses, as this information is needed by the hash partition bound search function.

## Parameters / Member Variables
- `step`: Base PartitionPruneStep structure containing type and step_id
- `opstrategy`: Strategy number of the operator in the clause matched to the last partition key, determining how partitions are selected
- `exprs`: List of expressions forming the lookup key passed to partition bound search function
- `cmpfns`: List of OIDs for comparison functions used to compare expressions with partition bounds (same length as exprs)
- `nullkeys`: Bitmapset containing offsets of partition keys (0 to partnatts-1) matched to IS NULL clauses, used for hash partitioning

## Dependencies
- Functions called/Symbols referenced:
  - PartitionPruneStep (base structure)
  - StrategyNumber (operator strategy system)
  - List (PostgreSQL list structure)
  - Bitmapset (PostgreSQL bitmap structure)

- Called from (representative examples):
  - InitPartitionPruneContext (src/backend/executor/execPartition.c:2130)
  - get_matching_partitions (src/backend/partitioning/partprune.c:852)
  - gen_prune_step_op (src/backend/partitioning/partprune.c:1318)
  - perform_pruning_base_step (src/backend/partitioning/partprune.c:3417)

## Notes and Other Information
- Cannot have both an expression in exprs and corresponding bit set in nullkeys for the same partition key
- Handles up to partnatts items in both exprs and cmpfns lists
- Essential for PostgreSQL partitioned table query optimization
- Works with different partitioning schemes (range, hash, list)
- Part of the executor partition pruning system that runs during query execution