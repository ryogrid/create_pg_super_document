# array_agg_combine

## Location
[src/backend/utils/adt/array_userfuncs.c:525-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L525-L621)

## Overview
Combines two ArrayBuildState structures during parallel aggregate processing for array_agg(), merging their accumulated elements into a single state.

## Definition

```c
Datum
array_agg_combine(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a combine function used in parallel aggregation for the array_agg() aggregate function. It merges two ArrayBuildState structures (state1 and state2) that have been accumulated in different parallel workers into a single combined state. The function handles various scenarios including NULL states, empty states, and the need to expand arrays when combining states with different numbers of elements.

The function ensures proper memory management by copying data into the aggregate context and uses efficient memory allocation strategies (power of 2 sizing) when expanding arrays. It preserves both the actual data values and their null status indicators during the merge process.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : First ArrayBuildState to combine (may be NULL)
  - : Second ArrayBuildState to combine (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
  - [initArrayResultWithSize](../i/initArrayResultWithSize.md)
  - [datumCopy](../d/datumCopy.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [repalloc](../r/repalloc.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - memcpy
- Called from (representative examples):
  - PostgreSQL parallel aggregate framework (internal)

## Notes and Other Information
- This is specifically designed for parallel aggregation support in PostgreSQL
- Handles memory context switching to ensure data persists in the correct aggregate context
- Uses power-of-2 allocation strategy for efficient array growth
- Preserves element type consistency between states being combined
- Returns NULL only when both input states are NULL
- Essential for scaling array_agg() operations across multiple parallel workers

## Simplified Source

```c
Datum
array_agg_combine(PG_FUNCTION_ARGS)
{
    // Ensure aggregate context
    MemoryContext agg_context;
    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "aggregate function called in non-aggregate context");

    // Get the two states to combine
    ArrayBuildState *state1 = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
    ArrayBuildState *state2 = PG_ARGISNULL(1) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(1);

    // Handle NULL cases
    if (state2 == NULL) {
        if (state1 == NULL)
            PG_RETURN_NULL();
        PG_RETURN_POINTER(state1);
    }

    if (state1 == NULL) {
        // Copy state2 into aggregate context
        state1 = initArrayResultWithSize(state2->element_type, agg_context,
                                        false, state2->alen);
        MemoryContext old_context = MemoryContextSwitchTo(agg_context);

        // Copy all elements from state2
        for (int i = 0; i < state2->nelems; i++) {
            if (!state2->dnulls[i])
                state1->dvalues[i] = datumCopy(state2->dvalues[i],
                                             state1->typbyval, state1->typlen);
            else
                state1->dvalues[i] = (Datum) 0;
        }

        MemoryContextSwitchTo(old_context);
        memcpy(state1->dnulls, state2->dnulls, sizeof(bool) * state2->nelems);
        state1->nelems = state2->nelems;

        PG_RETURN_POINTER(state1);
    }

    // Combine both states if state2 has elements
    if (state2->nelems > 0) {
        int reqsize = state1->nelems + state2->nelems;
        MemoryContext oldContext = MemoryContextSwitchTo(state1->mcontext);

        // Expand state1 arrays if needed
        if (state1->alen < reqsize) {
            state1->alen = pg_nextpower2_32(reqsize);
            state1->dvalues = (Datum *) repalloc(state1->dvalues,
                                               state1->alen * sizeof(Datum));
            state1->dnulls = (bool *) repalloc(state1->dnulls,
                                             state1->alen * sizeof(bool));
        }

        // Copy state2 elements to end of state1
        for (int i = 0; i < state2->nelems; i++) {
            if (!state2->dnulls[i])
                state1->dvalues[i + state1->nelems] =
                    datumCopy(state2->dvalues[i], state1->typbyval, state1->typlen);
            else
                state1->dvalues[i + state1->nelems] = (Datum) 0;
        }

        memcpy(&state1->dnulls[state1->nelems], state2->dnulls,
               sizeof(bool) * state2->nelems);
        state1->nelems = reqsize;

        MemoryContextSwitchTo(oldContext);
    }

    PG_RETURN_POINTER(state1);
}
```