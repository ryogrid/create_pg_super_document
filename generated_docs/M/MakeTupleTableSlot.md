# MakeTupleTableSlot

## Location
[src/backend/executor/execTuples.c:1199-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1199-L1257)

## Overview
Creates and initializes an empty TupleTableSlot of a specified type, with optional tuple descriptor for optimized memory allocation and fixed lifetime schema.

## Definition
```c
TupleTableSlot *
MakeTupleTableSlot(TupleDesc tupleDesc,
                   const TupleTableSlotOps *tts_ops)
```

## Detailed Description
This function serves as the primary constructor for TupleTableSlot objects in PostgreSQL's execution engine. It creates a new slot instance based on the provided slot operations structure, which determines the specific slot type and its behavior.

The function implements an important memory optimization: when a fixed tuple descriptor is provided, it allocates the entire slot structure, including the Datum and isnull arrays, in a single memory allocation. This reduces memory overhead and improves cache locality by placing related data structures contiguously in memory.

The function initializes all essential slot fields including the operations pointer, node type, flags, memory context, and validity counters. It also handles the reference counting for the tuple descriptor when provided and delegates type-specific initialization to the slot's operations structure.

## Parameters / Member Variables
- `tupleDesc`: Optional TupleDesc that fixes the slot's schema for its lifetime (NULL for flexible slots)
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the slot type and its operations

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - PinTupleDesc
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md)->init
  - MAXALIGN (macro)
  - CurrentMemoryContext (global)
- Called from (representative examples):
  - [ExecPartitionCheckEmitError](../E/ExecPartitionCheckEmitError.md)
  - [ExecConstraints](../E/ExecConstraints.md)
  - [ExecWithCheckOptions](../E/ExecWithCheckOptions.md)
  - [ExecAllocTableSlot](../E/ExecAllocTableSlot.md)
  - [MakeSingleTupleTableSlot](MakeSingleTupleTableSlot.md)

## Notes and Other Information
- When tupleDesc is provided, the slot gains TTS_FLAG_FIXED flag and optimized single-allocation layout
- The function uses MAXALIGN to ensure proper memory alignment for all data structures
- Type-specific initialization is delegated to the slot operations structure's init function
- The function handles reference counting for tuple descriptors automatically
- Memory allocation uses palloc0 to zero-initialize all fields for safety
- The slot's memory context is set to the current context at creation time
- This is the foundational function for all slot creation in the PostgreSQL executor

## Simplified Source

```c
TupleTableSlot *
MakeTupleTableSlot(TupleDesc tupleDesc, const TupleTableSlotOps *tts_ops)
{
    Size basesz, allocsz;
    TupleTableSlot *slot;

    basesz = tts_ops->base_slot_size;

    // Optimize memory allocation when tuple descriptor is fixed
    if (tupleDesc)
        allocsz = MAXALIGN(basesz) +
                  MAXALIGN(tupleDesc->natts * sizeof(Datum)) +
                  MAXALIGN(tupleDesc->natts * sizeof(bool));
    else
        allocsz = basesz;

    slot = palloc0(allocsz);

    // Initialize basic slot properties
    *((const TupleTableSlotOps **) &slot->tts_ops) = tts_ops;
    slot->type = T_TupleTableSlot;
    slot->tts_flags |= TTS_FLAG_EMPTY;
    if (tupleDesc != NULL)
        slot->tts_flags |= TTS_FLAG_FIXED;
    slot->tts_tupleDescriptor = tupleDesc;
    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_nvalid = 0;

    // Set up optimized arrays when using fixed descriptor
    if (tupleDesc != NULL)
    {
        slot->tts_values = (Datum *)(((char *) slot) + MAXALIGN(basesz));
        slot->tts_isnull = (bool *)(((char *) slot) + MAXALIGN(basesz) +
                                    MAXALIGN(tupleDesc->natts * sizeof(Datum)));
        PinTupleDesc(tupleDesc);
    }

    // Call slot-type specific initialization
    slot->tts_ops->init(slot);

    return slot;
}
```