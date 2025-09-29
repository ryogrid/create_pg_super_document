# ExecFetchSlotHeapTupleDatum

## Location
[src/backend/executor/execTuples.c:1810-1841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1810-L1841)

## Overview
Fetches a TupleTableSlot's content as a composite-type Datum, providing a serialized representation suitable for storage or transmission.

## Definition
```c
Datum ExecFetchSlotHeapTupleDatum(TupleTableSlot *slot)
```

## Detailed Description
This function converts the contents of a TupleTableSlot into a composite-type Datum, which is a serialized form that can be stored, passed as a function parameter, or transmitted between processes. The function first fetches the slot's contents as a HeapTuple using ExecFetchSlotHeapTuple, then converts that HeapTuple into Datum form using heap_copy_tuple_as_datum.

The result is always freshly allocated in the caller's memory context, ensuring the caller has ownership of the returned Datum. This is the inverse operation of ExecStoreHeapTupleDatum - while that function takes a Datum and stores it in a slot, this function takes a slot and converts it to a Datum.

## Parameters / Member Variables
- `slot`: The TupleTableSlot containing the tuple data to convert to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTuple](ExecFetchSlotHeapTuple.md)
  - [heap_copy_tuple_as_datum](../h/heap_copy_tuple_as_datum.md)
  - [pfree](../p/pfree.md) (conditionally)
- Called from (representative examples):
  - [ExecMakeFunctionResultSet](ExecMakeFunctionResultSet.md)
  - [postquel_get_single_result](../p/postquel_get_single_result.md)
  - TupIsNull

## Notes and Other Information
- The result is always palloc'd in the caller's memory context, so the caller is responsible for freeing it
- Uses ExecFetchSlotHeapTuple with materialize=false for efficiency
- Properly handles memory management by freeing the intermediate HeapTuple if needed
- Commonly used when tuple data needs to be passed as a composite-type argument to functions
- Part of PostgreSQL's type system that allows tuples to be treated as first-class values
- The resulting Datum represents the entire tuple as a single composite value that can be manipulated by the type system

## Simplified Source

```c
Datum ExecFetchSlotHeapTupleDatum(TupleTableSlot *slot)
{
    HeapTuple tup;
    TupleDesc tupdesc;
    bool shouldFree;
    Datum ret;

    // Step 1: Get the slot's contents as a HeapTuple
    tup = ExecFetchSlotHeapTuple(slot, false, &shouldFree);
    tupdesc = slot->tts_tupleDescriptor;

    // Step 2: Convert HeapTuple to Datum format
    ret = heap_copy_tuple_as_datum(tup, tupdesc);

    // Step 3: Clean up intermediate HeapTuple if needed
    if (shouldFree) {
        pfree(tup);
    }

    return ret;  // Freshly allocated composite-type Datum
}
```