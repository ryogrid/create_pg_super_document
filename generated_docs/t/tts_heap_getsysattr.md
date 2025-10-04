# tts_heap_getsysattr

## Location
[src/backend/executor/execTuples.c:355-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L355-L374)

## Overview
tts_heap_getsysattr retrieves system attribute values from a HeapTupleTableSlot by delegating to heap_getsysattr after validating the slot contains a materialized tuple.

## Definition
```c
static Datum
tts_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
```

## Detailed Description
This function implements the getsysattr callback for heap tuple table slots within the TupleTableSlotOps interface. It provides access to PostgreSQL system attributes (like ctid, xmin, xmax, cmin, cmax, tableoid) for heap tuples. The function performs validation checks to ensure the slot is not empty and contains a materialized heap tuple before delegating to heap_getsysattr. If the slot is not materialized (hslot->tuple is NULL), it raises an error since system attributes cannot be retrieved from virtual tuples.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot containing the heap tuple (cast to HeapTupleTableSlot internally)
- `attnum`: System attribute number to retrieve (negative values for system attributes)
- `isnull`: Pointer to boolean flag that will be set to indicate if the attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [HeapTupleTableSlot](../H/HeapTupleTableSlot.md) (cast target type)
  - TTS_EMPTY (macro to check if slot is empty)
  - [heap_getsysattr](../h/heap_getsysattr.md) (function that retrieves system attributes from heap tuples)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (indirectly through TupleTableSlotOps structure)

## Notes and Other Information
- This function only works with materialized heap tuples, not virtual or minimal tuples
- Raises ERRCODE_FEATURE_NOT_SUPPORTED error when called on non-materialized slots
- System attributes include metadata like transaction IDs, tuple identifiers, and table OIDs
- The actual system attribute extraction logic is implemented in heap_getsysattr
- Critical for queries that need access to tuple metadata beyond user-defined columns
- Part of PostgreSQL's system catalog and MVCC (Multi-Version Concurrency Control) infrastructure

## Simplified Source

```c
static Datum
tts_heap_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
    HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) slot;

    // Ensure slot is not empty
    Assert(!TTS_EMPTY(slot));

    // Check if tuple is materialized - system columns require physical tuple
    if (!hslot->tuple)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot retrieve a system column in this context")));

    // Delegate to heap-specific system attribute retrieval
    return heap_getsysattr(hslot->tuple, attnum, slot->tts_tupleDescriptor, isnull);
}
```