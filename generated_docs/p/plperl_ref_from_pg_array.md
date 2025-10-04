# plperl_ref_from_pg_array

## Location
[src/pl/plperl/plperl.c:1480-1558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1480-L1558)

## Overview
Converts a PostgreSQL array datum to a Perl array reference, handling multi-dimensional arrays and preserving type information.

## Definition
static SV *plperl_ref_from_pg_array(Datum arg, Oid typid)

## Detailed Description
This function transforms PostgreSQL array data into Perl data structures that can be manipulated in PL/Perl code. It handles arrays of any dimension and element type, preserving both the array structure and type metadata. The function decomposes the PostgreSQL array using the array API, extracts individual elements, and recursively builds a corresponding Perl array structure. The result is wrapped in a blessed Perl object that includes both the array data and the original PostgreSQL type OID for potential round-trip conversions. Element conversion is handled through either transform functions (if available) or standard output functions.

## Parameters / Member Variables
- `arg`: PostgreSQL Datum containing the array data to be converted
- `typid`: PostgreSQL type OID of the array type (must be an array type)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_ELEMTYPE
  - ARR_NDIM
  - ARR_DIMS
  - [get_type_io_data](../g/get_type_io_data.md)
  - [get_transform_fromsql](../g/get_transform_fromsql.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [type_is_rowtype](../t/type_is_rowtype.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [split_array](../s/split_array.md)
  - newRV_noinc
  - newSVuv
- Called from (representative examples):
  - [plperl_call_perl_func](plperl_call_perl_func.md)
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md)

## Notes and Other Information
- Returns a blessed Perl object of class "PostgreSQL::InServer::ARRAY"
- The returned object contains both "array" and "typeoid" keys for complete type preservation
- Handles empty arrays (zero dimensions) by returning an empty Perl array reference
- Uses transform functions when available for element conversion, falling back to output functions
- Currently does not cache type information lookups, which could impact performance
- Supports multi-dimensional arrays through recursive splitting via split_array function
- Memory allocation uses PostgreSQL memory context for intermediate data structures

## Simplified Source

```c
static SV *
plperl_ref_from_pg_array(Datum arg, Oid typid)
{
    // Extract array data from the datum
    ArrayType *pg_array = DatumGetArrayTypeP(arg);
    Oid elementtype = ARR_ELEMTYPE(pg_array);

    // Setup array processing information
    plperl_array_info *info = palloc0(sizeof(plperl_array_info));

    // Get element type information for conversion
    int16 typlen;
    bool typbyval;
    char typalign, typdelim;
    Oid typioparam, typoutputfunc;
    get_type_io_data(elementtype, IOFunc_output,
                     &typlen, &typbyval, &typalign,
                     &typdelim, &typioparam, &typoutputfunc);

    // Check for custom transform function for element type
    Oid transform_funcid = get_transform_fromsql(elementtype,
                                                current_call_data->prodesc->lang_oid,
                                                current_call_data->prodesc->trftypes);

    // Setup conversion function (transform or output)
    if (OidIsValid(transform_funcid)) {
        fmgr_info(transform_funcid, &info->transform_proc);
    } else {
        fmgr_info(typoutputfunc, &info->proc);
    }

    info->elem_is_rowtype = type_is_rowtype(elementtype);

    // Get array dimension information
    info->ndims = ARR_NDIM(pg_array);
    int *dims = ARR_DIMS(pg_array);

    SV *perl_array;

    if (info->ndims == 0) {
        // Empty array case
        perl_array = newRV_noinc((SV *) newAV());
    } else {
        // Deconstruct PostgreSQL array into individual elements
        int nitems;
        deconstruct_array(pg_array, elementtype, typlen, typbyval,
                          typalign, &info->elements, &info->nulls, &nitems);

        // Calculate element counts for each dimension
        info->nelems = palloc(sizeof(int) * info->ndims);
        info->nelems[0] = nitems;
        for (int i = 1; i < info->ndims; i++) {
            info->nelems[i] = info->nelems[i - 1] / dims[i - 1];
        }

        // Build the multidimensional Perl array structure
        perl_array = split_array(info, 0, nitems, 0);
    }

    // Create blessed object with array data and type information
    HV *blessed_hash = newHV();
    hv_store(blessed_hash, "array", 5, perl_array, 0);
    hv_store(blessed_hash, "typeoid", 7, newSVuv(typid), 0);

    return sv_bless(newRV_noinc((SV *) blessed_hash),
                    gv_stashpv("PostgreSQL::InServer::ARRAY", 0));
}
```