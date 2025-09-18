# ExecFetchSlotHeapTupleDatum

## Location
src/backend/executor/execTuples.c: 1810 - 1841

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
  - ExecFetchSlotHeapTuple
  - heap_copy_tuple_as_datum
  - pfree (conditionally)
- Called from (representative examples):
  - ExecMakeFunctionResultSet
  - postquel_get_single_result
  - TupIsNull

## Notes and Other Information
- The result is always palloc'd in the caller's memory context, so the caller is responsible for freeing it
- Uses ExecFetchSlotHeapTuple with materialize=false for efficiency
- Properly handles memory management by freeing the intermediate HeapTuple if needed
- Commonly used when tuple data needs to be passed as a composite-type argument to functions
- Part of PostgreSQL's type system that allows tuples to be treated as first-class values
- The resulting Datum represents the entire tuple as a single composite value that can be manipulated by the type system