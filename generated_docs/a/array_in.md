# array_in

## Location
[src/backend/utils/adt/arrayfuncs.c:179-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L179-L401)

## Overview
Converts an array from its external string format to the internal PostgreSQL ArrayType representation, serving as the input conversion function for PostgreSQL array types.

## Definition

```c
struct_empty_array(element_type));
```
## Detailed Description
The  function is the primary input conversion function for PostgreSQL arrays. It parses a string representation of an array (e.g., "{1,2,3}" or "[1:3]={1,2,3}") and converts it into PostgreSQL's internal ArrayType structure. The function handles multi-dimensional arrays with optional explicit dimension specifications and lower bounds.

The function implements a sophisticated parsing strategy that:
1. Caches element type metadata (ArrayMetaState) for performance across multiple calls
2. Parses optional dimension information using ReadArrayDimensions
3. Parses array values using ReadArrayStr 
4. Constructs the final ArrayType structure with proper memory layout
5. Handles null values, data alignment, and size validation

The parsing supports two formats:
- Simple format: "{val1,val2,val3}"
- Explicit dimensions: "[lower:upper]={val1,val2,val3}"

## Parameters / Member Variables
- : External string representation of the array to parse
- : OID of the array's element type
- : Type modifier for array elements
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ReadArrayDimensions](../R/ReadArrayDimensions.md)
  - [ReadArrayStr](../R/ReadArrayStr.md)  
  - [get_type_io_data](../g/get_type_io_data.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - ARR_OVERHEAD_WITHNULLS/ARR_OVERHEAD_NONULLS
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [extract_variadic_args](../e/extract_variadic_args.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
- Uses ArrayMetaState caching to optimize repeated calls with the same element type
- Supports arrays up to MAXDIM dimensions
- Handles both NULL values and variable-length elements properly
- Performs extensive validation including size overflow checks
- The function is registered in the PostgreSQL type system as the input function for array types
- Memory layout follows PostgreSQL's ArrayType format with optional null bitmap

## Simplified Source

```c
Datum
array_in(PG_FUNCTION_ARGS)
{
    char       *string = PG_GETARG_CSTRING(0);        // input string
    Oid         element_type = PG_GETARG_OID(1);      // element type OID
    int32       typmod = PG_GETARG_INT32(2);          // type modifier
    Node       *escontext = fcinfo->context;          // error context

    int         typlen, ndim, nitems;
    bool        typbyval, hasnulls;
    char        typalign, typdelim;
    Oid         typioparam;
    char       *p;
    Datum      *values;
    bool       *nulls;
    int32       nbytes, dataoffset;
    ArrayType  *retval;
    int         dim[MAXDIM], lBound[MAXDIM];
    ArrayMetaState *my_extra;

    // Cache element type information for efficiency
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->element_type != element_type) {
        // Initialize or update cached type info
        if (my_extra == NULL) {
            fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                          sizeof(ArrayMetaState));
            my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        }

        get_type_io_data(element_type, IOFunc_input,
                        &my_extra->typlen, &my_extra->typbyval,
                        &my_extra->typalign, &my_extra->typdelim,
                        &my_extra->typioparam, &my_extra->typiofunc);
        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                      fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    // Extract cached type properties
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;
    typdelim = my_extra->typdelim;
    typioparam = my_extra->typioparam;

    // Initialize dimension info with defaults
    for (int i = 0; i < MAXDIM; i++) {
        dim[i] = -1;        // unknown dimension
        lBound[i] = 1;      // default lower bound
    }

    // Parse dimension information if present (e.g., "[1:3]")
    p = string;
    if (!ReadArrayDimensions(&p, &ndim, dim, lBound, string, escontext))
        return (Datum) 0;

    // Handle dimension syntax: either starts with '{' or has '=' after dimensions
    if (ndim == 0) {
        if (*p != '{')
            ereturn(escontext, (Datum) 0, /* error: must start with '{' */);
    } else {
        if (strncmp(p, ASSGN, strlen(ASSGN)) != 0)
            ereturn(escontext, (Datum) 0, /* error: missing '=' */);
        p += strlen(ASSGN);
        while (scanner_isspace(*p)) p++;
        if (*p != '{')
            ereturn(escontext, (Datum) 0, /* error: must start with '{' */);
    }

    // Parse array values from "{...}" section
    if (!ReadArrayStr(&p, &my_extra->proc, typioparam, typmod, typdelim,
                      typlen, typbyval, typalign, &ndim, dim,
                      &nitems, &values, &nulls, string, escontext))
        return (Datum) 0;

    // Validate no junk after closing brace
    while (*p) {
        if (!scanner_isspace(*p++))
            ereturn(escontext, (Datum) 0, /* error: junk after '}' */);
    }

    // Handle empty arrays
    if (nitems == 0)
        PG_RETURN_ARRAYTYPE_P(construct_empty_array(element_type));

    // Calculate space requirements and check for nulls
    hasnulls = false;
    nbytes = 0;
    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            hasnulls = true;
        } else {
            if (typlen == -1)
                values[i] = PointerGetDatum(PG_DETOAST_DATUM(values[i]));
            nbytes = att_addlength_datum(nbytes, typlen, values[i]);
            nbytes = att_align_nominal(nbytes, typalign);
            if (!AllocSizeIsValid(nbytes))
                ereturn(escontext, (Datum) 0, /* error: array too large */);
        }
    }

    // Calculate total size including headers
    if (hasnulls) {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
        nbytes += dataoffset;
    } else {
        dataoffset = 0;
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

    // Copy element data into array
    CopyArrayEls(retval, values, nulls, nitems,
                 typlen, typbyval, typalign, true);

    pfree(values);
    pfree(nulls);

    PG_RETURN_ARRAYTYPE_P(retval);
}
```