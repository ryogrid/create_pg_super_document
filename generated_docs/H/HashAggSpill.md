# HashAggSpill

## Location
src/backend/executor/nodeAgg.c: 331 - 339

## Overview
HashAggSpill represents partitioned spill data for a single hashtable in PostgreSQL's hash aggregation implementation, containing necessary information to route tuples to correct partitions and transform spilled data into new batches.

## Definition


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
  - LogicalTape
  - hyperLogLogState
- Called from (representative examples):
  - hash_agg_enter_spill_mode
  - lookup_hash_entries
  - agg_refill_hash_table
  - hashagg_spill_init
  - hashagg_spill_tuple
  - hashagg_finish_initial_spills
  - hashagg_spill_finish
  - hashagg_reset_spill_state

## Notes and Other Information
- Used as part of the AggState structure (src/include/nodes/execnodes.h:2509)
- The partitioning scheme supports recursive spilling by using different bit ranges of hash values at different levels
- HyperLogLog cardinality estimation helps optimize memory management and processing decisions
- LogicalTape provides the underlying temporary storage abstraction for spilled data