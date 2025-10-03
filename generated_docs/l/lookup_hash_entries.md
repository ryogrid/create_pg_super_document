# lookup_hash_entries

## Location
[src/backend/executor/nodeAgg.c:2095-2157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2095-L2157)

## Overview
Looks up hash entries for the current tuple across all hashed grouping sets, handling both in-memory and spill-mode scenarios in hash aggregation.

## Definition

```c
static void
lookup_hash_entries(AggState *aggstate)
```
## Detailed Description
This function processes the current input tuple by looking it up in hash tables for all active grouping sets. For each grouping set, it prepares the hash key, searches the corresponding hash table, and either finds an existing entry or creates a new one (if not in spill mode). When the hash table has been spilled to disk, new entries are not created; instead, the tuple is written to the appropriate spill partition. The function handles the complexity of multiple grouping sets where the same tuple may belong to different groups in each set - some groups may be in memory while others may have been spilled. This design allows for efficient partitioned processing during hash table refill operations.

## Parameters / Member Variables
- `*aggstate`: The AggState structure containing all aggregation execution state and hash tables
## Dependencies
- Functions called/Symbols referenced:
  - [select_current_set](../s/select_current_set.md)
  - [prepare_hash_slot](../p/prepare_hash_slot.md)
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md)
  - [initialize_hash_entry](../i/initialize_hash_entry.md)
  - [hashagg_spill_init](../h/hashagg_spill_init.md)
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - [AggState](../A/AggState.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [AggStatePerHash](../A/AggStatePerHash.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - [TupleHashEntry](../T/TupleHashEntry.md)
  - [HashAggSpill](../H/HashAggSpill.md)
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_fill_hash_table](../a/agg_fill_hash_table.md)

## Notes and Other Information
- The function may reset tmpcontext during hash entry lookup operations
- In spill mode, new hash entries are not created; tuples are instead written to spill partitions
- The same tuple may be spilled multiple times for different grouping sets, which enables efficient partitioned refilling
- Each grouping set maintains its own hash table and spill state
- The pergroup array is updated with either the hash entry's additional data or NULL for spilled tuples
- Spill partitions are lazily initialized when first needed for a grouping set
- The hash value computed during lookup is reused for spilling operations when needed

## Simplified Source

```c
static void lookup_hash_entries(AggState *aggstate) {
    AggStatePerGroup *pergroup = aggstate->hash_pergroup;
    TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
    int setno;

    for (setno = 0; setno < aggstate->num_hashes; setno++) {
        AggStatePerHash perhash = &aggstate->perhash[setno];
        TupleHashTable hashtable = perhash->hashtable;
        TupleTableSlot *hashslot = perhash->hashslot;
        TupleHashEntry entry;
        uint32 hash;
        bool isnew = false;
        bool *p_isnew;

        // Don't create new entries if in spill mode
        p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;

        // Prepare hash key for this grouping set
        select_current_set(aggstate, setno, true);
        prepare_hash_slot(perhash, outerslot, hashslot);

        // Look up or create hash entry
        entry = LookupTupleHashEntry(hashtable, hashslot, p_isnew, &hash);

        if (entry != NULL) {
            // Found or created entry in hash table
            if (isnew)
                initialize_hash_entry(aggstate, hashtable, entry);
            pergroup[setno] = entry->additional;
        } else {
            // Spill tuple to disk
            HashAggSpill *spill = &aggstate->hash_spills[setno];
            TupleTableSlot *slot = aggstate->tmpcontext->ecxt_outertuple;

            if (spill->partitions == NULL)
                hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
                                   perhash->aggnode->numGroups,
                                   aggstate->hashentrysize);

            hashagg_spill_tuple(aggstate, spill, slot, hash);
            pergroup[setno] = NULL;
        }
    }
}
```