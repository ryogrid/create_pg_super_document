# array_send

## Location
[src/backend/utils/adt/arrayfuncs.c:1548-1651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1548-L1651)

## Overview
Converts PostgreSQL arrays from internal ArrayType representation to external binary format for network transmission or storage serialization.

## Definition

```c
Datum
array_send(PG_FUNCTION_ARGS)
```
## Detailed Description
array_send is the binary send function for PostgreSQL arrays, responsible for serializing internal ArrayType structures into binary format suitable for network transmission or storage. The function uses PostgreSQL's standard binary protocol format, which includes array metadata (dimensions, bounds, null flags) followed by individual element data. It employs a caching mechanism (ArrayMetaState) to optimize performance across multiple function calls with the same element type.

The serialization process involves writing array header information (dimensions, null flag, element type, dimension sizes and bounds), followed by individual elements using element-specific send procedures. NULL elements are represented with a special -1 length marker, while non-null elements include their data length followed by the actual binary data. The function ensures proper memory management by freeing temporary bytea objects created during element serialization.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P
  - AARR_ELEMTYPE
  - AARR_NDIM
  - AARR_DIMS
  - AARR_LBOUND
  - AARR_HASNULL
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [get_type_io_data](../g/get_type_io_data.md)
  - IOFunc_send
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [array_iter_setup](array_iter_setup.md)
  - [array_iter_next](array_iter_next.md)
  - [SendFunctionCall](../S/SendFunctionCall.md)
  - [pq_sendbytes](../p/pq_sendbytes.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - VARSIZE
  - VARDATA
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - [int2vectorsend](../i/int2vectorsend.md)
  - [oidvectorsend](../o/oidvectorsend.md)
  - [anyarray_send](anyarray_send.md)
  - [anycompatiblearray_send](anycompatiblearray_send.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function uses ArrayMetaState caching to store element type information and avoid repeated type system lookups, improving performance for repeated operations. The binary protocol format is PostgreSQL-specific and includes comprehensive metadata to enable proper deserialization. NULL elements are efficiently represented using -1 length markers without additional data. The function ensures that element types have valid binary send procedures and reports appropriate errors for types lacking binary output support. Memory management is handled carefully with proper cleanup of temporary bytea objects to prevent memory leaks during serialization.

## Simplified Source

```c
Datum
array_send(PG_FUNCTION_ARGS)
{
    AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
    Oid element_type = AARR_ELEMTYPE(v);
    int typlen, nitems, i;
    bool typbyval;
    char typalign;
    int ndim, *dim, *lb;
    StringInfoData buf;
    array_iter iter;
    ArrayMetaState *my_extra;

    // Cache element type information for performance
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->element_type != element_type) {
        if (my_extra == NULL) {
            fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                         sizeof(ArrayMetaState));
            my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        }

        // Get element type I/O information for binary send
        get_type_io_data(element_type, IOFunc_send,
                        &my_extra->typlen, &my_extra->typbyval,
                        &my_extra->typalign, &my_extra->typdelim,
                        &my_extra->typioparam, &my_extra->typiofunc);

        if (!OidIsValid(my_extra->typiofunc))
            ereport(ERROR, /* no binary send function */);

        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                     fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    // Extract array metadata
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;
    ndim = AARR_NDIM(v);
    dim = AARR_DIMS(v);
    lb = AARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dim);

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Send array header: dimensions, null flag, element type
    pq_sendint32(&buf, ndim);
    pq_sendint32(&buf, AARR_HASNULL(v) ? 1 : 0);
    pq_sendint32(&buf, element_type);

    // Send dimension sizes and lower bounds
    for (i = 0; i < ndim; i++) {
        pq_sendint32(&buf, dim[i]);
        pq_sendint32(&buf, lb[i]);
    }

    // Send array elements using element's send procedure
    array_iter_setup(&iter, v);

    for (i = 0; i < nitems; i++) {
        Datum itemvalue;
        bool isnull;

        // Get array element
        itemvalue = array_iter_next(&iter, &isnull, i,
                                   typlen, typbyval, typalign);

        if (isnull) {
            // -1 length indicates NULL element
            pq_sendint32(&buf, -1);
        } else {
            // Convert element to binary and send with length prefix
            bytea *outputbytes = SendFunctionCall(&my_extra->proc, itemvalue);
            pq_sendint32(&buf, VARSIZE(outputbytes) - VARHDRSZ);
            pq_sendbytes(&buf, VARDATA(outputbytes),
                        VARSIZE(outputbytes) - VARHDRSZ);
            pfree(outputbytes);
        }
    }

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```