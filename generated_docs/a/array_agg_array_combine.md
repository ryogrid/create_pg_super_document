# array_agg_array_combine

## Location
[src/backend/utils/adt/array_userfuncs.c:901-1049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L901-L1049)

## Overview
Combines two ArrayBuildStateArr states during parallel aggregation of array_agg operations, merging accumulated arrays from different worker processes.

## Definition
Datum array_agg_array_combine(PG_FUNCTION_ARGS)

## Detailed Description
This function is used as a combine function for the array_agg aggregate when running in parallel mode. It takes two ArrayBuildStateArr states (representing partial aggregation results from different parallel workers) and combines them into a single state. The function handles memory management by ensuring all data is moved to the aggregation context, validates that the arrays being combined have compatible dimensions, and efficiently merges the data and null bitmaps.

The function implements the following logic:
- If either state is NULL, returns the non-NULL state (or NULL if both are NULL)
- If state1 is NULL but state2 has data, copies state2's data into the aggregation context
- If both states have data, validates dimensional compatibility and merges them by concatenating data, combining null bitmaps, and updating dimension information

## Parameters / Member Variables
- : Function call information structure containing the two ArrayBuildStateArr pointers as arguments
- Returns: Combined ArrayBuildStateArr state as a Datum pointer

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [initArrayResultArr](../i/initArrayResultArr.md)
  - [array_bitmap_copy](array_bitmap_copy.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - [repalloc](../r/repalloc.md)
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - No direct references found (used as aggregate combine function)

## Notes and Other Information
- This function is specifically designed for parallel aggregation of array_agg
- Ensures dimensional compatibility by checking that all dimensions except the first match exactly
- Uses power-of-2 allocation strategy for efficient memory management
- Handles null bitmap management for proper NULL value tracking in arrays
- All memory allocations are done in the aggregation context to ensure proper cleanup

## Simplified Source

```c
Datum
array_agg_array_combine(PG_FUNCTION_ARGS)
{
    // Ensure aggregate context
    MemoryContext agg_context;
    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "aggregate function called in non-aggregate context");

    // Get the two states to combine
    ArrayBuildStateArr *state1 = PG_ARGISNULL(0) ? NULL :
                                (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
    ArrayBuildStateArr *state2 = PG_ARGISNULL(1) ? NULL :
                                (ArrayBuildStateArr *) PG_GETARG_POINTER(1);

    // Handle NULL cases
    if (state2 == NULL) {
        if (state1 == NULL)
            PG_RETURN_NULL();
        PG_RETURN_POINTER(state1);
    }

    if (state1 == NULL) {
        // Copy state2 into aggregate context
        MemoryContext old_context = MemoryContextSwitchTo(agg_context);

        state1 = initArrayResultArr(state2->array_type, InvalidOid,
                                   agg_context, false);
        state1->abytes = state2->abytes;
        state1->data = (char *) palloc(state1->abytes);

        // Copy null bitmap if present
        if (state2->nullbitmap) {
            int size = (state2->aitems + 7) / 8;
            state1->nullbitmap = (bits8 *) palloc(size);
            memcpy(state1->nullbitmap, state2->nullbitmap, size);
        }

        // Copy all state data
        memcpy(state1->data, state2->data, state2->nbytes);
        state1->nbytes = state2->nbytes;
        state1->aitems = state2->aitems;
        state1->nitems = state2->nitems;
        state1->ndims = state2->ndims;
        memcpy(state1->dims, state2->dims, sizeof(state2->dims));
        memcpy(state1->lbs, state2->lbs, sizeof(state2->lbs));
        state1->array_type = state2->array_type;
        state1->element_type = state2->element_type;

        MemoryContextSwitchTo(old_context);
        PG_RETURN_POINTER(state1);
    }

    // Combine both states if state2 has items
    if (state2->nitems > 0) {
        // Validate dimensional compatibility
        if (state1->ndims != state2->ndims)
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                           errmsg("cannot accumulate arrays of different dimensionality")));

        for (int i = 1; i < state1->ndims; i++) {
            if (state1->dims[i] != state2->dims[i] || state1->lbs[i] != state2->lbs[i])
                ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                               errmsg("cannot accumulate arrays of different dimensionality")));
        }

        MemoryContext oldContext = MemoryContextSwitchTo(state1->mcontext);
        int reqsize = state1->nbytes + state2->nbytes;

        // Expand data buffer if needed
        if (state1->abytes < reqsize) {
            state1->abytes = pg_nextpower2_32(reqsize);
            state1->data = (char *) repalloc(state1->data, state1->abytes);
        }

        // Handle null bitmap merging
        if (state2->nullbitmap) {
            int newnitems = state1->nitems + state2->nitems;

            if (state1->nullbitmap == NULL) {
                // First input with nulls - mark previous items non-null
                state1->aitems = pg_nextpower2_32(Max(256, newnitems + 1));
                state1->nullbitmap = (bits8 *) palloc((state1->aitems + 7) / 8);
                array_bitmap_copy(state1->nullbitmap, 0, NULL, 0, state1->nitems);
            } else if (newnitems > state1->aitems) {
                int newaitems = state1->aitems + state2->aitems;
                state1->aitems = pg_nextpower2_32(newaitems);
                state1->nullbitmap = (bits8 *) repalloc(state1->nullbitmap,
                                                        (state1->aitems + 7) / 8);
            }
            array_bitmap_copy(state1->nullbitmap, state1->nitems,
                             state2->nullbitmap, 0, state2->nitems);
        }

        // Merge data and update counters
        memcpy(state1->data + state1->nbytes, state2->data, state2->nbytes);
        state1->nbytes += state2->nbytes;
        state1->nitems += state2->nitems;
        state1->dims[0] += state2->dims[0];

        MemoryContextSwitchTo(oldContext);
    }

    PG_RETURN_POINTER(state1);
}
```