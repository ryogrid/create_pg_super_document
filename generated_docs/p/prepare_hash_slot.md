# prepare_hash_slot

## Location
[src/backend/executor/nodeAgg.c:1204-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1204-L1248)

## Overview
Extracts the attributes that make up the grouping key from the input tuple into a hash slot, which is necessary for computing hash values or performing hash table lookups.

## Definition
```c
static inline void prepare_hash_slot(AggStatePerHash perhash,
                                    TupleTableSlot *inputslot,
                                    TupleTableSlot *hashslot)
```

## Detailed Description
This function prepares a hash slot by transferring only the needed grouping columns from the input tuple slot to a dedicated hash slot. It first ensures that the required attributes are fetched from the input slot, then clears the hash slot and copies only the grouping key columns. The function is optimized to transfer only the necessary columns rather than the entire tuple, improving performance in hash-based grouping operations.

## Parameters / Member Variables
- `perhash`: Per-hash aggregate state containing grouping column information and indices
- `inputslot`: The input tuple slot containing the source data
- `hashslot`: The destination slot that will contain only the grouping key attributes

## Dependencies
- Functions called/Symbols referenced:
  - [slot_getsomeattrs](../s/slot_getsomeattrs.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
- Called from (representative examples):
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)

## Notes and Other Information
- Marked as inline for performance optimization since it's called frequently during hash operations
- Only transfers the columns specified in hashGrpColIdxInput array, not the entire tuple
- Uses slot_getsomeattrs with largestGrpColIdx to ensure all needed attributes are materialized
- The resulting hash slot contains a virtual tuple with only the grouping key columns
- Critical for hash-based aggregate processing where grouping keys need to be efficiently compared and hashed