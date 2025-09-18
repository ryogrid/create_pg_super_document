# array_agg_deserialize

## Location
src/backend/utils/adt/array_userfuncs.c: 711 - 821

## Overview
Deserializes a bytea-encoded ArrayBuildState structure back into memory during parallel aggregate processing for array_agg().

## Definition


## Detailed Description
This function reconstructs an ArrayBuildState structure from its serialized bytea representation, which was created by array_agg_serialize(). It reads the binary data stream to extract metadata (element type, count, type properties) and rebuilds both the data values and null indicators arrays.

The function handles different deserialization strategies based on element type characteristics: for by-value types, it directly copies the Datum array from the serialized data, while for by-reference types, it uses the element type's binary input function to properly reconstruct each element. The function includes validation to ensure data integrity and caches input function information for efficiency.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : bytea pointer containing the serialized ArrayBuildState data

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
  - PG_GETARG_BYTEA_PP
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [initArrayResultWithSize](../i/initArrayResultWithSize.md)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md)
  - [DeserialIOData](../D/DeserialIOData.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [ReceiveFunctionCall](../R/ReceiveFunctionCall.md)
  - [pq_getmsgend](../p/pq_getmsgend.md)
  - memcpy
  - ereport
- Called from (representative examples):
  - PostgreSQL parallel aggregate framework (internal)

## Notes and Other Information
- Companion function to array_agg_serialize() for parallel aggregation support
- Validates serialized data integrity and reports errors for corrupted data
- Caches type input function information in fn_extra for performance optimization
- Handles both by-value and by-reference element types with appropriate reconstruction methods
- Skips deserialization of null elements for by-reference types to maintain efficiency
- Creates the result ArrayBuildState in the current memory context
- Essential for enabling array_agg() to work correctly across parallel worker boundaries
- Includes bounds checking to prevent buffer overflow attacks on malformed input