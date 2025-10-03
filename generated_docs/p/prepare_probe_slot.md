# prepare_probe_slot

## Location
[src/backend/executor/nodeMemoize.c:302-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L302-L343)

## Overview
Populates the MemoizeState's probeslot with key values either from a cached MemoizeKey or by evaluating parameter expressions for cache lookups.

## Definition
```c
static inline void prepare_probe_slot(MemoizeState *mstate, MemoizeKey *key)
```

## Detailed Description
This function prepares the probeslot for cache operations by populating it with key values through one of two methods:

1. **From MemoizeKey (key != NULL)**: Extracts values from the provided key's MinimalTuple by storing it in tableslot, extracting all attributes, and copying the Datum values and null flags to probeslot.

2. **From parameter expressions (key == NULL)**: Evaluates the mstate's param_exprs array to compute current parameter values and stores them directly in probeslot.

After populating the values and null flags, the function calls ExecStoreVirtualTuple() to finalize the slot state. The probeslot is then ready for use in hash table operations like hashing and equality comparisons.

## Parameters / Member Variables
- `mstate`: Pointer to MemoizeState containing probeslot, tableslot, param_exprs, and other necessary context
- `key`: MemoizeKey containing cached tuple parameters (NULL means evaluate from current parameters)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoizeKey](../M/MemoizeKey.md)
  - [MemoizeState](../M/MemoizeState.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [ExecStoreMinimalTuple](../E/ExecStoreMinimalTuple.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
- Called from (representative examples):
  - [cache_reduce_memory](../c/cache_reduce_memory.md)
  - [cache_lookup](../c/cache_lookup.md)
  - [cache_store_tuple](../c/cache_store_tuple.md)

## Notes and Other Information
- Always starts by clearing the probeslot with ExecClearTuple()
- Uses memory context switching to ecxt_per_tuple_memory when evaluating expressions
- When copying from a key, uses memcpy for efficient bulk copying of values and null flags
- The probeslot must be prepared before any hash table operations (hashing, equality checks)
- Supports both cached key restoration and fresh parameter evaluation scenarios
- The inline keyword suggests this is a performance-critical function called frequently

## Simplified Source

```c
static inline void
prepare_probe_slot(MemoizeState *mstate, MemoizeKey *key)
{
    TupleTableSlot *pslot = mstate->probeslot;
    TupleTableSlot *tslot = mstate->tableslot;
    int numKeys = mstate->nkeys;

    // Clear the probe slot for fresh data
    ExecClearTuple(pslot);

    if (key == NULL)
    {
        // Evaluate current parameter expressions
        ExprContext *econtext = mstate->ss.ps.ps_ExprContext;
        MemoryContext oldcontext;

        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

        // Evaluate each parameter expression
        for (int i = 0; i < numKeys; i++)
            pslot->tts_values[i] = ExecEvalExpr(mstate->param_exprs[i],
                                                econtext,
                                                &pslot->tts_isnull[i]);

        MemoryContextSwitchTo(oldcontext);
    }
    else
    {
        // Extract values from cached key's MinimalTuple
        ExecStoreMinimalTuple(key->params, tslot, false);
        slot_getallattrs(tslot);

        // Copy values and null flags to probe slot
        memcpy(pslot->tts_values, tslot->tts_values, sizeof(Datum) * numKeys);
        memcpy(pslot->tts_isnull, tslot->tts_isnull, sizeof(bool) * numKeys);
    }

    // Finalize the virtual tuple
    ExecStoreVirtualTuple(pslot);
}
```