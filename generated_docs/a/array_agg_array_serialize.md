# array_agg_array_serialize

## Location
[src/backend/utils/adt/array_userfuncs.c:1050-1108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1050-L1108)

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
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendbytes](../p/pq_sendbytes.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
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

## Simplified Source

```c
Datum
array_agg_array_serialize(PG_FUNCTION_ARGS)
{
    // Ensure aggregate context
    Assert(AggCheckCallContext(fcinfo, NULL));

    ArrayBuildStateArr *state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    // Begin serialization
    pq_begintypsend(&buf);

    // Send metadata first for easier deserialization
    pq_sendint32(&buf, state->element_type);  // element type first
    pq_sendint32(&buf, state->array_type);    // array type
    pq_sendint32(&buf, state->nbytes);        // data size

    // Send actual array data
    pq_sendbytes(&buf, state->data, state->nbytes);

    // Send allocation and item tracking info
    pq_sendint32(&buf, state->abytes);        // allocated buffer size
    pq_sendint32(&buf, state->aitems);        // allocated item count

    // Send null bitmap if present
    if (state->nullbitmap) {
        Assert(state->aitems > 0);
        pq_sendbytes(&buf, state->nullbitmap, (state->aitems + 7) / 8);
    }

    // Send item count and dimensionality info
    pq_sendint32(&buf, state->nitems);        // actual item count
    pq_sendint32(&buf, state->ndims);         // number of dimensions

    // Send dimension and lower bounds arrays
    pq_sendbytes(&buf, state->dims, sizeof(state->dims));
    pq_sendbytes(&buf, state->lbs, sizeof(state->lbs));

    // Finalize and return serialized bytea
    bytea *result = pq_endtypsend(&buf);
    PG_RETURN_BYTEA_P(result);
}
```