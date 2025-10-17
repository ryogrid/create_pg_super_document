# array_agg_deserialize

## Location
[src/backend/utils/adt/array_userfuncs.c:711-821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L711-L821)

## Overview
Deserializes a bytea-encoded ArrayBuildState structure back into memory during parallel aggregate processing for array_agg().

## Definition

```c
structure.
	 */
	initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate),
						   VARSIZE_ANY_EXHDR(sstate));
```
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

## Simplified Source

```c
Datum
array_agg_deserialize(PG_FUNCTION_ARGS)
{
    // Ensure aggregate context
    if (!AggCheckCallContext(fcinfo, NULL))
        elog(ERROR, "aggregate function called in non-aggregate context");

    bytea *sstate = PG_GETARG_BYTEA_PP(0);
    StringInfoData buf;

    // Initialize read buffer from serialized data
    initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

    // Read metadata from serialized stream
    Oid element_type = pq_getmsgint(&buf, 4);
    int64 nelems = pq_getmsgint64(&buf);

    // Create output ArrayBuildState with correct size
    ArrayBuildState *result = initArrayResultWithSize(element_type,
                                                     CurrentMemoryContext,
                                                     false, nelems);
    result->nelems = nelems;
    result->typlen = pq_getmsgint(&buf, 2);
    result->typbyval = pq_getmsgbyte(&buf);
    result->typalign = pq_getmsgbyte(&buf);

    // Read null flags array
    const char *temp = pq_getmsgbytes(&buf, sizeof(bool) * nelems);
    memcpy(result->dnulls, temp, sizeof(bool) * nelems);

    // Read data values based on type characteristics
    if (result->typbyval) {
        // For by-value types, copy raw Datum array
        temp = pq_getmsgbytes(&buf, sizeof(Datum) * nelems);
        memcpy(result->dvalues, temp, sizeof(Datum) * nelems);
    } else {
        // For by-reference types, use type's receive function
        DeserialIOData *iodata = (DeserialIOData *) fcinfo->flinfo->fn_extra;

        // Cache type receive function info
        if (iodata == NULL) {
            Oid typreceive;
            iodata = (DeserialIOData *) MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                          sizeof(DeserialIOData));
            getTypeBinaryInputInfo(element_type, &typreceive, &iodata->typioparam);
            fmgr_info_cxt(typreceive, &iodata->typreceive, fcinfo->flinfo->fn_mcxt);
            fcinfo->flinfo->fn_extra = (void *) iodata;
        }

        // Deserialize each non-null element
        for (int i = 0; i < nelems; i++) {
            if (result->dnulls[i]) {
                result->dvalues[i] = (Datum) 0;
                continue;
            }

            // Read element length and validate bounds
            int itemlen = pq_getmsgint(&buf, 4);
            if (itemlen < 0 || itemlen > (buf.len - buf.cursor))
                ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                               errmsg("insufficient data left in message")));

            // Create element buffer and deserialize
            StringInfoData elem_buf;
            initReadOnlyStringInfo(&elem_buf, &buf.data[buf.cursor], itemlen);
            buf.cursor += itemlen;

            result->dvalues[i] = ReceiveFunctionCall(&iodata->typreceive,
                                                   &elem_buf,
                                                   iodata->typioparam, -1);
        }
    }

    pq_getmsgend(&buf);
    PG_RETURN_POINTER(result);
}
```