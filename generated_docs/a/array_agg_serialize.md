# array_agg_serialize

## Location
src/backend/utils/adt/array_userfuncs.c: 622 - 710

## Overview
Serializes an ArrayBuildState structure into a bytea format for transmission during parallel aggregate processing in array_agg().

## Definition


## Detailed Description
This function converts an ArrayBuildState structure into a serialized bytea format that can be transmitted between parallel workers during array_agg() processing. The serialization process includes metadata about the array elements (type information, length, alignment) as well as the actual data values and null indicators.

The function handles two different serialization strategies depending on whether the element type is passed by value (typbyval) or by reference. For by-value types, it directly transmits the Datum array. For by-reference types, it uses the element type's binary output function to properly serialize each non-null element, caching the output function information for efficiency.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : ArrayBuildState pointer to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - ArrayBuildState
  - pq_begintypsend
  - pq_sendint32
  - pq_sendint64
  - pq_sendint16
  - pq_sendbyte
  - pq_sendbytes
  - SerialIOData
  - MemoryContextAlloc
  - getTypeBinaryOutputInfo
  - fmgr_info_cxt
  - SendFunctionCall
  - pq_endtypsend
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - PostgreSQL parallel aggregate framework (internal)

## Notes and Other Information
- Used exclusively in parallel aggregation contexts for array_agg()
- Optimizes serialization by caching type output function information in fn_extra
- Handles both by-value and by-reference data types with appropriate serialization strategies
- Skips null elements for by-reference types to reduce serialization overhead
- The serialized format includes element type, count, type metadata, null flags, and actual data values
- Pairs with array_agg_deserialize to enable complete state transfer between parallel workers