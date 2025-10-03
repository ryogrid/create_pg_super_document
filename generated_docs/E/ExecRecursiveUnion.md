# ExecRecursiveUnion

## Location
[src/backend/executor/nodeRecursiveunion.c:75-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeRecursiveunion.c#L75-L166)

## Overview
Executes recursive UNION queries by processing non-recursive and recursive terms iteratively, implementing PostgreSQL's recursive Common Table Expression (CTE) functionality.

## Definition

```c
structure
	 */
	rustate = makeNode(RecursiveUnionState);
```
## Detailed Description
The `ExecRecursiveUnion` function implements the core logic for executing recursive UNION queries in PostgreSQL. It follows a two-phase approach: first evaluating the non-recursive term (anchor) to establish the initial result set, then iteratively executing the recursive term until no new tuples are generated.

The algorithm operates as follows:
1. **Non-recursive phase**: Evaluates the outer plan (non-recursive term) and populates both the working table and result set, using a hash table for duplicate elimination when specified.
2. **Recursive phase**: Iteratively executes the inner plan (recursive term) using the working table as input, generating new tuples that become the next iteration's working set.

The function manages three key data structures: a working table (current iteration's input), an intermediate table (next iteration's input), and a hash table for duplicate detection. After each recursive iteration, the intermediate table becomes the new working table, and the process continues until no new tuples are produced.

## Parameters / Member Variables
- `pstate`: Pointer to the PlanState structure, cast to RecursiveUnionState for recursive union execution context

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNode](ExecProcNode.md)
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md)
  - [tuplestore_end](../t/tuplestore_end.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [bms_add_member](../b/bms_add_member.md)
  - TupIsNull
  - outerPlanState
  - innerPlanState
- Called from (representative examples):
  - [ExecInitRecursiveUnion](ExecInitRecursiveUnion.md)

## Notes and Other Information
- Supports optional duplicate elimination through hash table lookups when numCols > 0
- Uses tuple stores for managing working and intermediate tables efficiently
- Implements proper memory context management with temp context resets after hash lookups
- Handles parameter changes for recursive term re-evaluation through chgParam bitmap
- Returns NULL when recursion terminates (no more tuples to process)
- Critical component of PostgreSQL's WITH RECURSIVE implementation

## Simplified Source

```c
static TupleTableSlot *
ExecRecursiveUnion(PlanState *pstate)
{
    RecursiveUnionState *node = castNode(RecursiveUnionState, pstate);
    PlanState *outerPlan = outerPlanState(node);
    PlanState *innerPlan = innerPlanState(node);
    RecursiveUnion *plan = (RecursiveUnion *) node->ps.plan;
    TupleTableSlot *slot;
    bool isnew;

    CHECK_FOR_INTERRUPTS();

    // Phase 1: Process non-recursive term (anchor)
    if (!node->recursing) {
        for (;;) {
            slot = ExecProcNode(outerPlan);
            if (TupIsNull(slot))
                break;

            // Check for duplicates if needed
            if (plan->numCols > 0) {
                LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL);
                MemoryContextReset(node->tempContext);
                if (!isnew)
                    continue; // Skip duplicates
            }

            // Add tuple to working table and return to caller
            tuplestore_puttupleslot(node->working_table, slot);
            return slot;
        }
        node->recursing = true;
    }

    // Phase 2: Process recursive term iteratively
    for (;;) {
        slot = ExecProcNode(innerPlan);
        if (TupIsNull(slot)) {
            // If intermediate table is empty, recursion is complete
            if (node->intermediate_empty)
                break;

            // Swap tables: intermediate becomes new working table
            tuplestore_end(node->working_table);
            node->working_table = node->intermediate_table;
            node->intermediate_table = tuplestore_begin_heap(false, false, work_mem);
            node->intermediate_empty = true;

            // Reset recursive plan for next iteration
            innerPlan->chgParam = bms_add_member(innerPlan->chgParam, plan->wtParam);
            continue;
        }

        // Check for duplicates if needed
        if (plan->numCols > 0) {
            LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL);
            MemoryContextReset(node->tempContext);
            if (!isnew)
                continue; // Skip duplicates
        }

        // Add new tuple to intermediate table and return to caller
        node->intermediate_empty = false;
        tuplestore_puttupleslot(node->intermediate_table, slot);
        return slot;
    }

    return NULL; // Recursion complete
}
```