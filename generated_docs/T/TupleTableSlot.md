# TupleTableSlot

## Location
[src/include/executor/tuptable.h:114-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L114-L131)

## Overview
TupleTableSlot is the base structure representing a tuple in PostgreSQL's executor, providing a uniform interface for accessing tuple data regardless of the underlying storage format.

## Definition
```c
typedef struct TupleTableSlot
{
    NodeTag     type;
    uint16      tts_flags;          /* Boolean states */
    AttrNumber  tts_nvalid;         /* # of valid values in tts_values */
    const TupleTableSlotOps *const tts_ops; /* implementation of slot */
    TupleDesc   tts_tupleDescriptor; /* slot's tuple descriptor */
    Datum      *tts_values;         /* current per-attribute values */
    bool       *tts_isnull;         /* current per-attribute isnull flags */
    MemoryContext tts_mcxt;         /* slot itself is in this context */
    ItemPointerData tts_tid;        /* stored tuple's tid */
    Oid         tts_tableOid;       /* table oid of tuple */
} TupleTableSlot;
```

## Detailed Description
TupleTableSlot serves as the fundamental data structure for tuple representation in PostgreSQL's execution engine. It abstracts different tuple storage formats (heap tuples, minimal tuples, virtual tuples) through a polymorphic interface using the tts_ops function pointer table. The slot can hold both materialized tuple data in tts_values/tts_isnull arrays and maintain references to the underlying storage. This design enables efficient tuple processing while supporting various optimization strategies like lazy evaluation and different storage formats.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TupleTableSlot node
- `tts_flags`: Bit flags indicating slot state (TTS_FLAG_EMPTY, TTS_FLAG_SHOULDFREE, TTS_FLAG_SLOW, TTS_FLAG_FIXED)
- `tts_nvalid`: Number of valid entries in tts_values array (for lazy evaluation)
- `tts_ops`: Pointer to operations structure defining slot behavior
- `tts_tupleDescriptor`: Tuple descriptor defining the slot's row type
- `tts_values`: Array of attribute values (Datum format)
- `tts_isnull`: Array of NULL indicators for each attribute
- `tts_mcxt`: Memory context where the slot itself is allocated
- `tts_tid`: Item pointer (tuple identifier) for the tuple
- `tts_tableOid`: OID of the table this tuple belongs to

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlotOps](TupleTableSlotOps.md)
  - NodeTag
  - [TupleDesc](TupleDesc.md)
  - Datum
  - [MemoryContext](../M/MemoryContext.md)
  - [ItemPointerData](../I/ItemPointerData.md)
- Called from (representative examples):
  - [ExecProcNode](../E/ExecProcNode.md) functions
  - Executor state nodes
  - Table access methods
  - JIT compilation functions

## Notes and Other Information
- Flag definitions: TTS_FLAG_EMPTY (slot is empty), TTS_FLAG_SHOULDFREE (slot owns tuple memory), TTS_FLAG_SLOW (deform optimization state), TTS_FLAG_FIXED (fixed tuple descriptor)
- Field number definitions are provided for introspection and debugging
- The slot supports lazy evaluation where tts_values may not be fully populated until accessed
- Different slot implementations (virtual, heap, minimal, buffer) provide specialized behavior through tts_ops
- The slot is the primary interface between plan nodes in the executor tree