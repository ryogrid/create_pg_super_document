# initialize_hash_entry

## Location
[src/backend/executor/nodeAgg.c:2045-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2045-L2094)

## Overview
Initializes a newly-created hash table entry for hash aggregation by setting up per-group state and initializing all aggregate functions for the new tuple group.

## Definition

```c
static void
initialize_hash_entry(AggState *aggstate, TupleHashTable hashtable,
					  TupleHashEntry entry)
```
## Detailed Description
This function performs the initialization of a fresh TupleHashEntry that has just been created in the hash table. It increments the current group count, checks aggregation limits to potentially trigger spilling, and then allocates and initializes per-group state for all aggregate functions. The function handles the case where there are no aggregates (numtrans == 0) by returning early. For each aggregate transition function, it calls initialize_aggregate to set up the initial state. The per-group state is allocated in the hash table's memory context to ensure proper memory management.

## Parameters / Member Variables
- : The AggState structure containing the aggregation execution state
- : The TupleHashTable where the entry resides
- : The TupleHashEntry that needs to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - [hash_agg_check_limits](../h/hash_agg_check_limits.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [initialize_aggregate](initialize_aggregate.md)
  - [AggState](../A/AggState.md)
  - [TupleHashTable](../T/TupleHashTable.md)
  - [TupleHashEntry](../T/TupleHashEntry.md)
  - [AggStatePerGroup](../A/AggStatePerGroup.md)
  - [AggStatePerGroupData](../A/AggStatePerGroupData.md)
  - [AggStatePerTrans](../A/AggStatePerTrans.md)
- Called from (representative examples):
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)

## Notes and Other Information
- Increments hash_ngroups_current to track the total number of groups currently in the hash table
- Calls hash_agg_check_limits which may trigger spilling to disk if memory or group limits are exceeded
- The per-group state allocation size is based on the number of transition functions (numtrans)
- Memory is allocated in the hash table's context (tablecxt) for proper lifecycle management
- The function assumes that lookup_hash_entries has already selected the appropriate grouping set
- Returns early if there are no aggregate functions to initialize (numtrans == 0)

## Simplified Source

```c
static void
initialize_hash_entry(AggState *aggstate, TupleHashTable hashtable,
                      TupleHashEntry entry)
{
    AggStatePerGroup pergroup;
    int transno;

    // Track new group and check memory limits
    aggstate->hash_ngroups_current++;
    hash_agg_check_limits(aggstate);

    // Early return if no aggregates to initialize
    if (aggstate->numtrans == 0)
        return;

    // Allocate per-group state for all aggregates
    pergroup = (AggStatePerGroup)
        MemoryContextAlloc(hashtable->tablecxt,
                          sizeof(AggStatePerGroupData) * aggstate->numtrans);

    entry->additional = pergroup;

    // Initialize each aggregate function for this group
    for (transno = 0; transno < aggstate->numtrans; transno++)
    {
        AggStatePerTrans pertrans = &aggstate->pertrans[transno];
        AggStatePerGroup pergroupstate = &pergroup[transno];

        initialize_aggregate(aggstate, pertrans, pergroupstate);
    }
}
```