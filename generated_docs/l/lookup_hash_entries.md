# lookup_hash_entries

## Location
src/backend/executor/nodeAgg.c: 2095 - 2157

## Overview
Looks up hash entries for the current tuple across all hashed grouping sets, handling both in-memory and spill-mode scenarios in hash aggregation.

## Definition


## Detailed Description
This function processes the current input tuple by looking it up in hash tables for all active grouping sets. For each grouping set, it prepares the hash key, searches the corresponding hash table, and either finds an existing entry or creates a new one (if not in spill mode). When the hash table has been spilled to disk, new entries are not created; instead, the tuple is written to the appropriate spill partition. The function handles the complexity of multiple grouping sets where the same tuple may belong to different groups in each set - some groups may be in memory while others may have been spilled. This design allows for efficient partitioned processing during hash table refill operations.

## Parameters / Member Variables
- : The AggState structure containing all aggregation execution state and hash tables

## Dependencies
- Functions called/Symbols referenced:
  - select_current_set
  - prepare_hash_slot
  - LookupTupleHashEntry
  - initialize_hash_entry
  - hashagg_spill_init
  - hashagg_spill_tuple
  - AggState
  - AggStatePerGroup
  - AggStatePerHash
  - TupleHashTable
  - TupleHashEntry
  - HashAggSpill
- Called from (representative examples):
  - agg_retrieve_direct
  - agg_fill_hash_table

## Notes and Other Information
- The function may reset tmpcontext during hash entry lookup operations
- In spill mode, new hash entries are not created; tuples are instead written to spill partitions
- The same tuple may be spilled multiple times for different grouping sets, which enables efficient partitioned refilling
- Each grouping set maintains its own hash table and spill state
- The pergroup array is updated with either the hash entry's additional data or NULL for spilled tuples
- Spill partitions are lazily initialized when first needed for a grouping set
- The hash value computed during lookup is reused for spilling operations when needed