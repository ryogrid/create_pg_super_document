# array_agg_array_deserialize

## Location
[src/backend/utils/adt/array_userfuncs.c:1109-1191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1109-L1191)

## Overview
Deserializes a bytea back into an ArrayBuildStateArr structure to reconstruct aggregation state received from parallel worker processes during array_agg operations.

## Definition
Datum array_agg_array_deserialize(PG_FUNCTION_ARGS)

## Detailed Description
This function is the counterpart to array_agg_array_serialize, taking a serialized bytea and reconstructing a complete ArrayBuildStateArr structure. It reads the binary data using PostgreSQL's message receive functions in the same order as the serialization, carefully reconstructing all components including array metadata, data buffers, null bitmaps, and dimension information. The function initializes a new ArrayBuildStateArr using initArrayResultArr and then populates it with the deserialized data, ensuring proper memory allocation for buffers based on the serialized size information.

## Parameters / Member Variables
- : Function call information structure containing the bytea as argument
- Returns: Reconstructed ArrayBuildStateArr as a Datum pointer

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - PG_GETARG_BYTEA_PP
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md)
  - [pq_getmsgend](../p/pq_getmsgend.md)
  - initArrayResultArr
  - [palloc](../p/palloc.md)
  - memcpy
- Called from (representative examples):
  - No direct references found (used as aggregate deserialize function)

## Notes and Other Information
- Cannot be called directly due to internal-type argument restriction
- Used specifically for parallel aggregation of array_agg operations
- Reconstructs complete state including proper buffer allocation
- Uses power-of-2 allocation strategy for data buffer sizing
- Null bitmap is conditionally reconstructed based on aitems count
- Element type is read first to enable early state initialization
- Ensures proper memory context allocation in CurrentMemoryContext
- Validates message completeness with pq_getmsgend