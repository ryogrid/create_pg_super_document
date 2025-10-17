# array_recv

## Location
[src/backend/utils/adt/arrayfuncs.c:1271-1453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1271-L1453)

## Overview
Converts PostgreSQL arrays from external binary format to their internal ArrayType representation, handling deserialization with comprehensive validation and type checking.

## Definition

```c
struct_empty_array(element_type));
```
## Detailed Description
array_recv is the binary receive function for PostgreSQL arrays, responsible for deserializing binary array data from network or storage formats into internal ArrayType structures. The function performs extensive validation of the binary input, including dimension bounds checking, element type verification, and format validation. It uses a caching mechanism (ArrayMetaState) to optimize repeated operations with the same element type.

The function processes the binary stream by reading array metadata (dimensions, bounds, flags), validates the element type against expected types (with special handling for built-in vs. user-defined types), reads individual array elements using ReadArrayBinary, and constructs the final ArrayType structure with proper null bitmap handling. Security considerations include robust validation to prevent malformed binary data from causing system issues.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro with three arguments:
  - buf (StringInfo): Binary data buffer containing the serialized array
  - spec_element_type (Oid): Expected element type OID
  - typmod (int32): Type modifier for array elements

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [get_type_io_data](../g/get_type_io_data.md)
  - IOFunc_receive
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [ReadArrayBinary](../R/ReadArrayBinary.md)
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - SET_VARSIZE
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - ARR_DIMS
  - ARR_LBOUND
  - PG_RETURN_ARRAYTYPE_P
- Called from (representative examples):
  - [int2vectorrecv](../i/int2vectorrecv.md)
  - [oidvectorrecv](../o/oidvectorrecv.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function implements security-conscious type checking, only complaining about type mismatches for built-in types (OIDs less than FirstGenbkiObjectId) since user-defined type OIDs are not stable across systems. It handles empty arrays as a special case, returning construct_empty_array() after validating the element type. The ArrayMetaState cache structure stores element type information to avoid repeated lookups. The function supports arrays with up to MAXDIM dimensions and performs comprehensive bounds checking to prevent integer overflow in array size calculations.

## Simplified Source

```c
Datum
array_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    Oid spec_element_type = PG_GETARG_OID(1);
    int32 typmod = PG_GETARG_INT32(2);

    Oid element_type;
    int typlen, nitems, i;
    bool typbyval, hasnulls;
    char typalign;
    Oid typioparam;
    Datum *dataPtr;
    bool *nullsPtr;
    int32 nbytes, dataoffset;
    ArrayType *retval;
    int ndim, flags, dim[MAXDIM], lBound[MAXDIM];
    ArrayMetaState *my_extra;

    // Read array header from binary stream
    ndim = pq_getmsgint(buf, 4);
    if (ndim < 0 || ndim > MAXDIM)
        ereport(ERROR, /* dimension validation error */);

    flags = pq_getmsgint(buf, 4);
    if (flags != 0 && flags != 1)
        ereport(ERROR, /* invalid flags error */);

    // Read and validate element type
    element_type = pq_getmsgint(buf, sizeof(Oid));
    if (element_type != spec_element_type) {
        // Only complain about mismatches for built-in types
        if (element_type < FirstGenbkiObjectId &&
            spec_element_type < FirstGenbkiObjectId)
            ereport(ERROR, /* type mismatch error */);
        element_type = spec_element_type;
    }

    // Read dimension sizes and lower bounds
    for (i = 0; i < ndim; i++) {
        dim[i] = pq_getmsgint(buf, 4);
        lBound[i] = pq_getmsgint(buf, 4);
    }

    // Validate array bounds and calculate total items
    nitems = ArrayGetNItems(ndim, dim);
    ArrayCheckBounds(ndim, dim, lBound);

    // Cache element type information for performance
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->element_type != element_type) {
        if (my_extra == NULL) {
            fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                         sizeof(ArrayMetaState));
            my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        }

        // Get element type I/O information for binary receive
        get_type_io_data(element_type, IOFunc_receive,
                        &my_extra->typlen, &my_extra->typbyval,
                        &my_extra->typalign, &my_extra->typdelim,
                        &my_extra->typioparam, &my_extra->typiofunc);

        if (!OidIsValid(my_extra->typiofunc))
            ereport(ERROR, /* no binary receive function */);

        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                     fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    // Handle empty array case
    if (nitems == 0) {
        PG_RETURN_ARRAYTYPE_P(construct_empty_array(element_type));
    }

    // Extract cached type parameters
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;
    typioparam = my_extra->typioparam;

    // Allocate storage for element data and null indicators
    dataPtr = (Datum *) palloc(nitems * sizeof(Datum));
    nullsPtr = (bool *) palloc(nitems * sizeof(bool));

    // Read all array elements from binary stream
    ReadArrayBinary(buf, nitems,
                   &my_extra->proc, typioparam, typmod,
                   typlen, typbyval, typalign,
                   dataPtr, nullsPtr,
                   &hasnulls, &nbytes);

    // Calculate array size including overhead
    if (hasnulls) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
        nbytes += dataoffset;
    } else {
        dataoffset = 0;  // no null bitmap
        nbytes += ARR_OVERHEAD_NONULLS(ndim);
    }

    // Construct final ArrayType structure
    retval = (ArrayType *) palloc0(nbytes);
    SET_VARSIZE(retval, nbytes);
    retval->ndim = ndim;
    retval->dataoffset = dataoffset;
    retval->elemtype = element_type;
    memcpy(ARR_DIMS(retval), dim, ndim * sizeof(int));
    memcpy(ARR_LBOUND(retval), lBound, ndim * sizeof(int));

    // Copy element data into array structure
    CopyArrayEls(retval, dataPtr, nullsPtr, nitems,
                typlen, typbyval, typalign, true);

    pfree(dataPtr);
    pfree(nullsPtr);

    PG_RETURN_ARRAYTYPE_P(retval);
}
```