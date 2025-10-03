# ExecMemoize

## Location
[src/backend/executor/nodeMemoize.c:697-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L697-L951)

## Overview
ExecMemoize is the main execution function for the Memoize node that caches and reuses query results based on parameter values to avoid redundant computation of expensive subplans.

## Definition

```c
static TupleTableSlot *
ExecMemoize(PlanState *pstate)
```
## Detailed Description
ExecMemoize implements a sophisticated caching mechanism for PostgreSQL's execution engine. It operates through a state machine with five distinct states:

1. **MEMO_CACHE_LOOKUP**: Initial state where it checks if results for current parameters are already cached
2. **MEMO_CACHE_FETCH_NEXT_TUPLE**: Returns subsequent tuples from a complete cache entry
3. **MEMO_FILLING_CACHE**: Actively populating cache while returning tuples from the outer plan
4. **MEMO_CACHE_BYPASS_MODE**: Passes through tuples without caching when memory constraints are exceeded
5. **MEMO_END_OF_SCAN**: Terminal state indicating no more tuples available

The function maintains statistics on cache hits, misses, and overflows to monitor caching effectiveness. When memory pressure occurs, it gracefully degrades to bypass mode rather than failing.

## Parameters / Member Variables
- `pstate`: The plan state node containing the MemoizeState structure with caching information, hash table, and current execution state

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - [build_hash_table](../b/build_hash_table.md)
  - [cache_lookup](../c/cache_lookup.md)
  - [cache_store_tuple](../c/cache_store_tuple.md)
  - [entry_purge_tuples](../e/entry_purge_tuples.md)
  - [ExecProcNode](ExecProcNode.md)
  - [ExecStoreMinimalTuple](ExecStoreMinimalTuple.md)
  - [ExecCopySlot](ExecCopySlot.md)
  - outerPlanState
  - TupIsNull
- Called from (representative examples):
  - [ExecInitMemoize](ExecInitMemoize.md) (sets up the execution function pointer)

## Notes and Other Information
- Uses a hash table-based cache with configurable estimated entries from the planner
- Handles incomplete cache entries by purging and rebuilding rather than attempting partial recovery
- Includes special handling for single-row expectations to optimize cache completion marking
- Implements graceful degradation to bypass mode when cache storage fails due to memory constraints
- Maintains comprehensive execution statistics for query optimization feedback

## Simplified Source

```c
static TupleTableSlot *
ExecMemoize(PlanState *pstate)
{
    MemoizeState *node = castNode(MemoizeState, pstate);
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    TupleTableSlot *slot;

    CHECK_FOR_INTERRUPTS();
    ResetExprContext(econtext);

    switch (node->mstatus) {
        case MEMO_CACHE_LOOKUP: {
            MemoizeEntry *entry;
            bool found;

            // Initialize hash table if needed
            if (unlikely(node->hashtable == NULL))
                build_hash_table(node, ((Memoize *) pstate->plan)->est_entries);

            // Look for cached results
            entry = cache_lookup(node, &found);

            if (found && entry->complete) {
                // Cache hit - return first cached tuple
                node->stats.cache_hits++;
                node->last_tuple = entry->tuplehead;
                node->entry = entry;

                if (entry->tuplehead) {
                    node->mstatus = MEMO_CACHE_FETCH_NEXT_TUPLE;
                    slot = node->ss.ps.ps_ResultTupleSlot;
                    ExecStoreMinimalTuple(entry->tuplehead->mintuple, slot, false);
                    return slot;
                }
                node->mstatus = MEMO_END_OF_SCAN;
                return NULL;
            }

            // Cache miss - fetch from outer plan
            node->stats.cache_misses++;
            if (found)
                entry_purge_tuples(node, entry);

            PlanState *outerNode = outerPlanState(node);
            TupleTableSlot *outerslot = ExecProcNode(outerNode);

            if (TupIsNull(outerslot)) {
                if (likely(entry))
                    entry->complete = true;
                node->mstatus = MEMO_END_OF_SCAN;
                return NULL;
            }

            node->entry = entry;

            // Try to cache the tuple
            if (unlikely(entry == NULL || !cache_store_tuple(node, outerslot))) {
                node->stats.cache_overflows++;
                node->mstatus = MEMO_CACHE_BYPASS_MODE;
            } else {
                entry->complete = node->singlerow;
                node->mstatus = MEMO_FILLING_CACHE;
            }

            slot = node->ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            return slot;
        }

        case MEMO_CACHE_FETCH_NEXT_TUPLE:
            // Return next cached tuple
            node->last_tuple = node->last_tuple->next;
            if (node->last_tuple == NULL) {
                node->mstatus = MEMO_END_OF_SCAN;
                return NULL;
            }
            slot = node->ss.ps.ps_ResultTupleSlot;
            ExecStoreMinimalTuple(node->last_tuple->mintuple, slot, false);
            return slot;

        case MEMO_FILLING_CACHE: {
            // Continue filling cache
            PlanState *outerNode = outerPlanState(node);
            TupleTableSlot *outerslot = ExecProcNode(outerNode);

            if (TupIsNull(outerslot)) {
                node->entry->complete = true;
                node->mstatus = MEMO_END_OF_SCAN;
                return NULL;
            }

            // Store tuple if possible
            if (unlikely(!cache_store_tuple(node, outerslot))) {
                node->stats.cache_overflows++;
                node->mstatus = MEMO_CACHE_BYPASS_MODE;
            }

            slot = node->ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            return slot;
        }

        case MEMO_CACHE_BYPASS_MODE: {
            // Bypass mode - no caching
            PlanState *outerNode = outerPlanState(node);
            TupleTableSlot *outerslot = ExecProcNode(outerNode);

            if (TupIsNull(outerslot)) {
                node->mstatus = MEMO_END_OF_SCAN;
                return NULL;
            }

            slot = node->ss.ps.ps_ResultTupleSlot;
            ExecCopySlot(slot, outerslot);
            return slot;
        }

        case MEMO_END_OF_SCAN:
            return NULL;

        default:
            elog(ERROR, "unrecognized memoize state: %d", (int) node->mstatus);
            return NULL;
    }
}
```