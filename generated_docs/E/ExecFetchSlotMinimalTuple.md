# ExecFetchSlotMinimalTuple

## Location
[src/backend/executor/execTuples.c:1779-1809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1779-L1809)

## Overview
Fetches a MinimalTuple representation of a TupleTableSlot's content, providing a compact physical tuple format with flexible memory management options.

## Definition
```c
MinimalTuple ExecFetchSlotMinimalTuple(TupleTableSlot *slot, bool *shouldFree)
```

## Detailed Description
This function retrieves the slot's content as a MinimalTuple, which is a compact physical tuple format used internally by PostgreSQL for operations like sorting and hashing. MinimalTuples are more space-efficient than HeapTuples as they omit some header information that's not needed in certain contexts.

The function uses the slot's operation vectors to determine the best approach: if a get_minimal_tuple operation is available, it returns the minimal tuple directly (owned by the slot, read-only). If not available, it uses copy_minimal_tuple to create a copy that the caller owns and can modify.

The shouldFree parameter indicates memory ownership - false means the slot owns the tuple (read-only), true means the caller owns it (can modify and must free).

## Parameters / Member Variables
- `slot`: The TupleTableSlot containing the tuple data to fetch
- `shouldFree`: Output parameter indicating whether caller must free the returned tuple

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro)
  - slot->tts_ops->get_minimal_tuple
  - slot->tts_ops->copy_minimal_tuple
- Called from (representative examples):
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - ExecHashTableInsert
  - ExecParallelHashTableInsert
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)
  - [tqueueReceiveSlot](../t/tqueueReceiveSlot.md)

## Notes and Other Information
- MinimalTuples are more compact than HeapTuples, making them suitable for memory-intensive operations
- When get_minimal_tuple is available, the returned tuple is read-only and owned by the slot
- When copy_minimal_tuple is used, the caller gets ownership and can modify the tuple
- Commonly used in hash tables, sorting operations, and inter-process communication where space efficiency is important
- Part of PostgreSQL's tuple slot abstraction system that provides different tuple representations optimized for different use cases
- The function performs sanity checks to ensure the slot is not NULL and not empty