# array_agg_array_serialize

## Location
src/backend/utils/adt/array_userfuncs.c: 1050 - 1108

## Overview
Serializes an ArrayBuildStateArr structure into a bytea format for transmission between parallel worker processes during array_agg aggregation.

## Definition
Datum array_agg_array_serialize(PG_FUNCTION_ARGS)

## Detailed Description
This function is used in parallel aggregation to serialize the internal state of an array_agg operation into a binary format that can be transmitted between processes. It converts an ArrayBuildStateArr structure into a bytea by systematically writing all the state components using PostgreSQL's type send functions. The serialization includes the array metadata (types, dimensions), actual data bytes, null bitmap information, and allocation tracking information needed to reconstruct the state in another process.

The serialization order is carefully chosen with element_type first to facilitate easier deserialization and state initialization in the receiving process.

## Parameters / Member Variables
- : Function call information structure containing the ArrayBuildStateArr pointer as argument
- Returns: Serialized state as a bytea Datum

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - pq_begintypsend
  - pq_sendint32
  - pq_sendbytes
  - pq_endtypsend
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found (used as aggregate serialize function)

## Notes and Other Information
- This function cannot be called directly due to its internal-type argument
- Used specifically for parallel aggregation of array_agg operations
- Serializes complete state including allocated buffer sizes and null bitmap
- The element_type is serialized first to enable efficient deserialization
- Null bitmap is only serialized if it exists (when aitems > 0)
- All dimension and lower bounds arrays are serialized completely regardless of actual dimensionality