# hashagg_spill_tuple

## Location
src/backend/executor/nodeAgg.c: 2925 - 2990

## Overview
Spills a tuple to disk when there is no room for new groups in the hash table, writing it to the appropriate partition based on its hash value.

## Definition
```c
static Size hashagg_spill_tuple(AggState *aggstate, HashAggSpill *spill, TupleTableSlot *inputslot, uint32 hash)
```

## Detailed Description
This function handles the spilling of individual tuples to disk when the in-memory hash table becomes full and cannot accommodate new groups. It determines the appropriate partition based on the tuple's hash value and writes both the hash and tuple data to the corresponding logical tape. The function optimizes storage by only spilling attributes that are actually needed for the aggregation operation.

Key operations performed:
1. Selects only necessary attributes if not all columns are needed (optimization)
2. Converts the tuple to MinimalTuple format for compact storage
3. Determines the target partition using hash-based partitioning
4. Updates cardinality estimates using HyperLogLog with hash redistribution
5. Writes both the hash value and tuple data to the partition's logical tape
6. Tracks the total bytes written for memory management

## Parameters / Member Variables
- `aggstate`: The aggregate node's execution state containing spill configuration and column information
- `spill`: HashAggSpill structure containing partition information and logical tapes
- `inputslot`: TupleTableSlot containing the tuple to be spilled
- `hash`: 32-bit hash value used for partition selection and cardinality estimation

## Dependencies
- Functions called/Symbols referenced:
  - slot_getsomeattrs
  - ExecClearTuple
  - bms_is_member
  - ExecStoreVirtualTuple
  - ExecFetchSlotMinimalTuple
  - addHyperLogLog
  - hash_bytes_uint32
  - LogicalTapeWrite
  - pfree
- Called from (representative examples):
  - lookup_hash_entries
  - agg_refill_hash_table

## Notes and Other Information
- The function implements column pruning optimization by only spilling needed attributes when `all_cols_needed` is false
- Hash values are rehashed using `hash_bytes_uint32` before adding to HyperLogLog to improve cardinality estimates
- Both the original hash value and tuple data are written to tape for later retrieval
- The function returns the total number of bytes written, which is used for memory accounting
- Partition selection uses bitwise operations with pre-calculated mask and shift values for efficiency
- MinimalTuple format is used for compact on-disk storage of spilled tuples