# setop_fill_hash_table

## Location
[src/backend/executor/nodeSetOp.c:339-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L339-L424)

## Overview
setop_fill_hash_table implements the first phase of the hashed strategy for set operations, reading all input tuples and building a hash table with group counts for subsequent retrieval.

## Definition

```c
static void
setop_fill_hash_table(SetOpState *setopstate)
```
## Detailed Description
This function builds a hash table for set operations when the hashed strategy is used (typically for unsorted inputs). The process works as follows:

1. **Input Processing**: Reads all tuples from the outer plan using ExecProcNode until exhausted
2. **Tuple Classification**: Uses fetch_tuple_flag to determine whether each tuple comes from the left or right input relation
3. **Hash Table Population**: 
   - For first relation tuples: Creates hash entries for new tuple groups and updates counts
   - For second relation tuples: Only updates counts for existing entries (doesn't create new entries for INTERSECT operations)
4. **Memory Management**: Allocates per-group count structures in the hash table's memory context
5. **Iterator Initialization**: Prepares the hash table iterator for subsequent retrieval phase

The function handles different set operation commands (INTERSECT, INTERSECT ALL, etc.) by applying different logic for first vs second relation processing. For INTERSECT operations, tuples from the second relation that don't match existing entries are ignored.

## Parameters / Member Variables
- : Pointer to the SetOpState structure containing the hash table, execution state, outer plan reference, and tuple processing context

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (gets outer plan state)
  - [ExecProcNode](../E/ExecProcNode.md) (executes outer plan to get next tuple)
  - TupIsNull (checks if tuple slot is empty)
  - [fetch_tuple_flag](../f/fetch_tuple_flag.md) (determines tuple's relation flag)
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md) (finds or creates hash table entry)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (allocates memory for per-group data)
  - [initialize_counts](../i/initialize_counts.md) (initializes per-group counters)
  - [advance_counts](../a/advance_counts.md) (updates counters for a tuple)
  - ResetExprContext (clears expression evaluation context)
  - ResetTupleHashIterator (initializes hash table iterator)
- Called from (representative examples):
  - [ExecSetOp](../E/ExecSetOp.md) (when using hashed strategy and table not yet filled)

## Notes and Other Information
- This is phase 1 of the two-phase hashed strategy (phase 2 is setop_retrieve_hash_table)
- Sets table_filled flag to true upon completion to prevent re-execution
- Uses assertions to verify planner constraints on firstFlag values
- Handles memory allocation in the hash table's memory context for proper cleanup
- Different behavior for first vs second input relations enables proper INTERSECT semantics
- Part of PostgreSQL's hashed strategy for set operations when input sorting is not available

## Simplified Source

```c
static void
setop_fill_hash_table(SetOpState *setopstate)
{
    SetOp *node = (SetOp *) setopstate->ps.plan;
    PlanState *outerPlan;
    int firstFlag;
    bool in_first_rel = true;
    ExprContext *econtext = setopstate->ps.ps_ExprContext;

    // Get state info from node
    outerPlan = outerPlanState(setopstate);
    firstFlag = node->firstFlag;

    // Process each tuple from outer plan
    for (;;) {
        TupleTableSlot *outerslot;
        int flag;
        TupleHashEntryData *entry;
        bool isnew;

        // Get next tuple
        outerslot = ExecProcNode(outerPlan);
        if (TupIsNull(outerslot))
            break;

        // Determine which input relation this tuple is from
        flag = fetch_tuple_flag(setopstate, outerslot);

        if (flag == firstFlag) {
            // Processing first input relation
            entry = LookupTupleHashEntry(setopstate->hashtable, outerslot,
                                         &isnew, NULL);

            // Initialize counts for new tuple groups
            if (isnew) {
                entry->additional = (SetOpStatePerGroup)
                    MemoryContextAlloc(setopstate->hashtable->tablecxt,
                                       sizeof(SetOpStatePerGroupData));
                initialize_counts((SetOpStatePerGroup) entry->additional);
            }

            // Update counts for this tuple
            advance_counts((SetOpStatePerGroup) entry->additional, flag);
        } else {
            // Processing second input relation
            in_first_rel = false;

            // Only update counts for existing entries (INTERSECT semantics)
            entry = LookupTupleHashEntry(setopstate->hashtable, outerslot,
                                         NULL, NULL);
            if (entry)
                advance_counts((SetOpStatePerGroup) entry->additional, flag);
        }

        // Reset expression context after each lookup
        ResetExprContext(econtext);
    }

    // Mark table as filled and initialize iterator
    setopstate->table_filled = true;
    ResetTupleHashIterator(setopstate->hashtable, &setopstate->hashiter);
}
```