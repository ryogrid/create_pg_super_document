# ExecStoreMinimalTuple

## Location
[src/backend/executor/execTuples.c:1533-1555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1533-L1555)

## Overview
Stores a MinimalTuple into a TTSOpsMinimalTuple type slot, providing an optimized path for storing minimal tuples when the target slot type is guaranteed to be compatible.

## Definition
```c
TupleTableSlot *ExecStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot *slot, bool shouldFree)
```

## Detailed Description
ExecStoreMinimalTuple is an optimized function for storing minimal tuples into tuple table slots that are specifically of the TTSOpsMinimalTuple type. Unlike its counterpart ExecForceStoreMinimalTuple, this function assumes the target slot is already the correct type and performs minimal validation, making it more efficient when the slot type is guaranteed. The function performs basic sanity checks and then delegates the actual storage operation to the low-level tts_minimal_store_tuple function.

The function includes a runtime check to ensure the slot is actually a minimal tuple slot type, throwing an error if this assumption is violated. This provides a safety net while maintaining the performance benefits of the optimized path.

## Parameters / Member Variables
- `mtup`: The MinimalTuple to be stored in the slot
- `slot`: The target TupleTableSlot where the minimal tuple will be stored (must be TTSOpsMinimalTuple type)
- `shouldFree`: Boolean flag indicating whether the slot should take ownership of the tuple memory and free it when appropriate

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (type)
  - TTS_IS_MINIMALTUPLE (macro for type checking)
  - [tts_minimal_store_tuple](../t/tts_minimal_store_tuple.md) (low-level storage function)
- Called from (representative examples):
  - TupleHashTableHash_internal
  - [TupleHashTableMatch](../T/TupleHashTableMatch.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)
  - [gather_getnext](../g/gather_getnext.md)
  - [ExecScanHashBucket](ExecScanHashBucket.md)
  - [tuplesort_gettupleslot](../t/tuplesort_gettupleslot.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)

## Notes and Other Information
- This function is optimized for performance when the slot type is guaranteed to be TTSOpsMinimalTuple
- For cases where the slot type is uncertain, use ExecForceStoreMinimalTuple instead, which handles type conversion
- The function includes an assertion-based safety check that will error if used with an incompatible slot type
- Related to tts_minimal_store_tuple which handles the actual low-level storage mechanics
- Part of PostgreSQL's tuple table slot system that provides type-specific optimizations for different tuple formats