# HashAggBatch

## Location
[src/backend/executor/nodeAgg.c:350-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L350-L357)

## Overview
HashAggBatch represents work to be done for one pass of hash aggregation with a single grouping set, tracking hash bits already used for partition selection to enable multi-level partitioning.

## Definition


## Detailed Description
HashAggBatch is a fundamental structure in PostgreSQL's multi-pass hash aggregation algorithm. Each batch represents a unit of work for processing spilled aggregation data. The structure tracks which bits of the hash value have already been consumed by previous partitioning levels through the used_bits field. This enables recursive partitioning where each level uses different bit ranges of the hash values. When all hash bits are exhausted, the batch will not perform further partitioning and any spilled data will be written to a single output tape. The structure encapsulates both the input source (tape) and metadata needed for processing decisions (tuple count, cardinality estimates).

## Parameters / Member Variables
- : Identifier for the grouping set being processed in this batch
- : Number of hash value bits already consumed by previous partitioning levels
- : LogicalTape containing the input data for this batch
- : Count of tuples contained in this batch
- : Estimated cardinality (number of distinct groups) for this batch

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](../L/LogicalTape.md)
- Called from (representative examples):
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - [hashagg_batch_new](../h/hashagg_batch_new.md)
  - [hashagg_batch_read](../h/hashagg_batch_read.md)
  - [hashagg_spill_finish](../h/hashagg_spill_finish.md)

## Notes and Other Information
- Essential component of PostgreSQL's recursive hash aggregation spilling strategy
- The used_bits field prevents infinite recursion by tracking hash bit consumption
- Works in conjunction with HashAggSpill for managing partitioned spill data
- Cardinality estimation helps optimize memory allocation and processing decisions
- When used_bits reaches the maximum, no further partitioning occurs to avoid infinite loops