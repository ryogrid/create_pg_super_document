# array_agg_serialize

## Location
[src/backend/utils/adt/array_userfuncs.c:622-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L622-L710)

## Overview
Serializes an ArrayBuildState structure into a bytea format for transmission during parallel aggregate processing in array_agg().

## Definition

```c
structure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));
```
## Detailed Description
This function converts an ArrayBuildState structure into a serialized bytea format that can be transmitted between parallel workers during array_agg() processing. The serialization process includes metadata about the array elements (type information, length, alignment) as well as the actual data values and null indicators.

The function handles two different serialization strategies depending on whether the element type is passed by value (typbyval) or by reference. For by-value types, it directly transmits the Datum array. For by-reference types, it uses the element type's binary output function to properly serialize each non-null element, caching the output function information for efficiency.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : ArrayBuildState pointer to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [ArrayBuildState](../A/ArrayBuildState.md)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendbytes](../p/pq_sendbytes.md)
  - [SerialIOData](../S/SerialIOData.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [SendFunctionCall](../S/SendFunctionCall.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
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

## Simplified Source

```c
Datum
array_agg_serialize(PG_FUNCTION_ARGS)
{
    // Ensure aggregate context
    Assert(AggCheckCallContext(fcinfo, NULL));

    ArrayBuildState *state = (ArrayBuildState *) PG_GETARG_POINTER(0);
    StringInfoData buf;

    // Begin serialization
    pq_begintypsend(&buf);

    // Send metadata first for deserialization
    pq_sendint32(&buf, state->element_type);  // element type
    pq_sendint64(&buf, state->nelems);        // number of elements
    pq_sendint16(&buf, state->typlen);        // type length
    pq_sendbyte(&buf, state->typbyval);       // pass by value flag
    pq_sendbyte(&buf, state->typalign);       // type alignment

    // Send null flags array
    pq_sendbytes(&buf, state->dnulls, sizeof(bool) * state->nelems);

    // Send actual data values
    if (state->typbyval) {
        // For by-value types, send raw Datum array
        pq_sendbytes(&buf, state->dvalues, sizeof(Datum) * state->nelems);
    } else {
        // For by-reference types, use type's send function
        SerialIOData *iodata = (SerialIOData *) fcinfo->flinfo->fn_extra;

        // Cache type send function info
        if (iodata == NULL) {
            Oid typsend;
            bool typisvarlena;

            iodata = (SerialIOData *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                        sizeof(SerialIOData));
            getTypeBinaryOutputInfo(state->element_type, &typsend, &typisvarlena);
            fmgr_info_cxt(typsend, &iodata->typsend, fcinfo->flinfo->fn_mcxt);
            fcinfo->flinfo->fn_extra = (void *) iodata;
        }

        // Serialize each non-null element
        for (int i = 0; i < state->nelems; i++) {
            if (state->dnulls[i])
                continue;  // Skip null elements

            bytea *outputbytes = SendFunctionCall(&iodata->typsend, state->dvalues[i]);
            pq_sendint32(&buf, VARSIZE(outputbytes) - VARHDRSZ);
            pq_sendbytes(&buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
        }
    }

    // Finalize and return serialized bytea
    bytea *result = pq_endtypsend(&buf);
    PG_RETURN_BYTEA_P(result);
}
```