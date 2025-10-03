# setop_retrieve_hash_table

## Location
[src/backend/executor/nodeSetOp.c:425-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L425-L480)

## Overview
setop_retrieve_hash_table implements the second phase of the hashed strategy for set operations, retrieving and returning result tuples from the previously built hash table.

## Definition

```c
static TupleTableSlot *
setop_retrieve_hash_table(SetOpState *setopstate)
```
## Detailed Description
This function implements phase 2 of the hashed set operation strategy, iterating through the hash table built by setop_fill_hash_table to produce result tuples. The process involves:

1. **Hash Table Iteration**: Uses ScanTupleHashTable to walk through all entries in the hash table
2. **Output Determination**: For each hash table entry, calls set_output_count to determine how many copies of the tuple should be returned based on the set operation type and the accumulated counts
3. **Tuple Return**: If the group should produce output, decrements the output counter and returns the tuple stored in the hash entry
4. **State Management**: Tracks completion through setop_done flag and manages output counting via numOutput

The function returns one tuple per call, maintaining state between calls to continue processing the same group if multiple copies are needed, or to move to the next hash table entry.

## Parameters / Member Variables
- `*setopstate`: Pointer to the SetOpState structure containing the hash table, hash iterator, result tuple slot, and output counting state
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - ScanTupleHashTable (iterates through hash table entries)
  - [set_output_count](set_output_count.md) (determines output count for a group)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md) (stores minimal tuple in result slot)
  - [ExecClearTuple](../E/ExecClearTuple.md) (clears tuple slot when done)
- Called from (representative examples):
  - [ExecSetOp](../E/ExecSetOp.md) (when using hashed strategy after table is filled)

## Notes and Other Information
- This is phase 2 of the two-phase hashed strategy (phase 1 is setop_fill_hash_table)
- Returns NULL when hash table iteration is complete (setop_done = true)
- Uses minimal tuple storage for efficiency since tuples are already materialized in hash table
- Handles multiple output copies through numOutput counter mechanism
- Processes hash table entries in hash order, not input order
- Part of PostgreSQL's hashed strategy for set operations when input data cannot be efficiently sorted

## Simplified Source

```c
static TupleTableSlot *
setop_retrieve_hash_table(SetOpState *setopstate)
{
    TupleHashEntryData *entry;
    TupleTableSlot *resultTupleSlot;

    // Get result slot
    resultTupleSlot = setopstate->ps.ps_ResultTupleSlot;

    // Process hash table entries until we find one to return
    while (!setopstate->setop_done) {
        CHECK_FOR_INTERRUPTS();

        // Get next entry from hash table
        entry = ScanTupleHashTable(setopstate->hashtable, &setopstate->hashiter);
        if (entry == NULL) {
            setopstate->setop_done = true;
            return NULL;
        }

        // Determine if this group should produce output
        set_output_count(setopstate, (SetOpStatePerGroup) entry->additional);

        if (setopstate->numOutput > 0) {
            setopstate->numOutput--;
            return ExecStoreMinimalTuple(entry->firstTuple,
                                         resultTupleSlot,
                                         false);
        }
    }

    // No more groups
    ExecClearTuple(resultTupleSlot);
    return NULL;
}
```