# AggStatePerHash

## Location
[src/include/nodes/execnodes.h:2461-2462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L2461-L2462)

## Overview
AggStatePerHash is a pointer type that represents per-hashtable state in PostgreSQL's hash aggregation implementation, managing hash tables for grouping operations.

## Definition

```c
typedef struct AggStatePerHashData *AggStatePerHash;
```
## Detailed Description
AggStatePerHash is a pointer to AggStatePerHashData structure that maintains execution state for individual hash tables used in hash-based aggregation operations. When performing grouping sets with hashing, PostgreSQL creates one AggStatePerHash instance for each grouping set. For regular hashing without grouping sets, only one instance is used. This structure encapsulates all the necessary components for hash table operations including the hash table itself, iteration state, hash functions, and column mapping information.

## Parameters / Member Variables
The underlying AggStatePerHashData structure contains:
- : TupleHashTable with one entry per group for storing aggregated results
- : TupleHashIterator for iterating through the hash table entries
- : TupleTableSlot for loading data into the hash table
- : FmgrInfo array containing hash functions for each grouping field
- : Oid array of equality function OIDs for each grouping field
- : Number of hash key columns used for grouping
- : Number of columns stored in the hash table tuples
- : Index of the largest column required for hashing operations
- : AttrNumber array mapping hash column indices in input slot
- : AttrNumber array mapping indices in hash table tuples
- : Pointer to original Agg node for accessing numGroups and other metadata

## Dependencies
- Functions called/Symbols referenced:
  - [AggStatePerHashData](AggStatePerHashData.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - TupleHashIterator
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - AttrNumber
  - [Agg](Agg.md)
- Called from (representative examples):
  - [prepare_hash_slot](../p/prepare_hash_slot.md)
  - [build_hash_tables](../b/build_hash_tables.md)
  - [build_hash_table](../b/build_hash_table.md)
  - [hash_agg_enter_spill_mode](../h/hash_agg_enter_spill_mode.md)
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)

## Notes and Other Information
This type is central to PostgreSQL's hash aggregation strategy and is particularly important when dealing with grouping sets where multiple hash tables may be needed simultaneously. The structure supports both in-memory and spill-to-disk operations for handling large datasets that exceed available memory.