# HashAggSpill

## Location
[src/backend/executor/nodeAgg.c:331-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L331-L339)

## Overview
HashAggSpill represents partitioned spill data for a single hashtable in PostgreSQL's hash aggregation implementation, containing necessary information to route tuples to correct partitions and transform spilled data into new batches.

## Definition

```c
typedef struct HashAggSpill
{
	int			npartitions;	/* number of partitions */
	LogicalTape **partitions;	/* spill partition tapes */
	int64	   *ntuples;		/* number of tuples in each partition */
	uint32		mask;			/* mask to find partition from hash value */
	int			shift;			/* after masking, shift by this amount */
	hyperLogLogState *hll_card; /* cardinality estimate for contents */
} HashAggSpill;
```
## Detailed Description
HashAggSpill is a core data structure used in PostgreSQL's hash aggregation spilling mechanism. When the hash table for aggregation becomes too large to fit in memory, PostgreSQL spills data to temporary storage partitioned across multiple logical tapes. This structure manages the partitioning scheme by using high bits of hash values for partition selection. During recursive processing, previously used bits are ignored to enable multi-level partitioning. The structure tracks both the physical storage (tapes) and metadata (tuple counts, cardinality estimates) needed to efficiently process spilled data.

## Parameters / Member Variables
- : Number of partitions used for spilling data
- : Array of pointers to LogicalTape structures representing spill partition tapes
- : Array tracking the number of tuples stored in each partition
- : Bitmask used to extract partition bits from hash values
- : Number of bits to shift after masking to get the final partition index
- : HyperLogLog state for estimating cardinality of spilled contents

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](../L/LogicalTape.md)
  - [hyperLogLogState](../h/hyperLogLogState.md)
- Called from (representative examples):
  - [hash_agg_enter_spill_mode](../h/hash_agg_enter_spill_mode.md)
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [hashagg_spill_init](../h/hashagg_spill_init.md)
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - [hashagg_finish_initial_spills](../h/hashagg_finish_initial_spills.md)
  - [hashagg_spill_finish](../h/hashagg_spill_finish.md)
  - [hashagg_reset_spill_state](../h/hashagg_reset_spill_state.md)

## Notes and Other Information
- Used as part of the AggState structure (src/include/nodes/execnodes.h:2509)
- The partitioning scheme supports recursive spilling by using different bit ranges of hash values at different levels
- HyperLogLog cardinality estimation helps optimize memory management and processing decisions
- [LogicalTape](../L/LogicalTape.md) provides the underlying temporary storage abstraction for spilled data