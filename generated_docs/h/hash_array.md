# hash_array

## Location
[src/backend/utils/adt/arrayfuncs.c:4146-4278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4146-L4278)

## Overview
PostgreSQL function that computes a hash value for an entire array by combining hash values of individual elements using a multiplicative hash algorithm.

## Definition

```c
structure.  Note that we can't just
			 * modify typentry, since that points directly into the type
			 * cache.
			 */
			record_typentry = palloc0(sizeof(*record_typentry));
```
## Detailed Description
The  function calculates a hash value for an array by iterating through all elements and combining their individual hash values using a multiplicative hash algorithm. It uses a rolling hash technique where each element's hash is combined using the formula: , which is equivalent to .

The function handles special cases including NULL elements (treated as having hash value 0) and record types. For record types, it creates a fake type cache entry since the type cache doesn't consider records hashable by default, but commits to hashing them anyway.

The hash algorithm provides good distribution properties for arrays up to 2^27 elements, where each element's hash value is multiplied by a different odd number in the cyclic group formed by powers of 31 modulo 2^32.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:

## Dependencies
- Functions called/Symbols referenced:
  -  - Get number of array dimensions
  -  - Get array dimension sizes
  -  - Get array element type OID
  -  - Get cached type information with hash function
  -  - Set up function manager info for record hashing
  -  - Calculate total number of array elements
  -  - [Initialize](../I/Initialize.md) array iterator
  -  - Get next array element
  -  - Call element hash function
  -  - Extract uint32 from Datum
  -  - Free detoasted array copies
  -  - Return hash result

- Called from (representative examples):
  - Used as hash support function for hash indexes on array columns
  - Called by hash-based operations and hash joins involving arrays

## Notes and Other Information
- Returns a 32-bit unsigned integer hash value
- Uses type cache to avoid repeated hash function lookups, improving performance for index operations
- NULL elements are assigned hash value 0 for consistent behavior
- Special handling for RECORD types by creating fake type cache entries
- [Hash](../H/Hash.md) algorithm provides good distribution for arrays with up to 134 million elements (2^27)
- Multiplicative constant 31 is chosen for good hash distribution properties
- Handles toasted arrays properly by freeing detoasted copies to prevent memory leaks
- The hash result incorporates all array elements but not array metadata (dimensions, bounds)

## Simplified Source

```c
Datum
hash_array(PG_FUNCTION_ARGS)
{
    LOCAL_FCINFO(locfcinfo, 1);
    AnyArrayType *array = PG_GETARG_ANY_ARRAY_P(0);

    // Extract array metadata
    int ndims = AARR_NDIM(array);
    int *dims = AARR_DIMS(array);
    Oid element_type = AARR_ELEMTYPE(array);
    uint32 result = 1;

    // Get cached hash function for element type
    TypeCacheEntry *typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
    if (typentry == NULL || typentry->type_id != element_type)
    {
        typentry = lookup_type_cache(element_type, TYPECACHE_HASH_PROC_FINFO);
        if (!OidIsValid(typentry->hash_proc_finfo.fn_oid) && element_type != RECORDOID)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("could not identify a hash function for type %s",
                                  format_type_be(element_type))));

        // Special handling for RECORD types
        if (element_type == RECORDOID)
        {
            MemoryContext oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
            TypeCacheEntry *record_typentry = palloc0(sizeof(*record_typentry));
            record_typentry->type_id = element_type;
            record_typentry->typlen = typentry->typlen;
            record_typentry->typbyval = typentry->typbyval;
            record_typentry->typalign = typentry->typalign;
            fmgr_info(F_HASH_RECORD, &record_typentry->hash_proc_finfo);
            MemoryContextSwitchTo(oldcontext);
            typentry = record_typentry;
        }
        fcinfo->flinfo->fn_extra = (void *) typentry;
    }

    // Setup element hashing
    InitFunctionCallInfoData(*locfcinfo, &typentry->hash_proc_finfo, 1,
                            PG_GET_COLLATION(), NULL, NULL);

    int nitems = ArrayGetNItems(ndims, dims);
    array_iter iter;
    array_iter_setup(&iter, array);

    // Hash each element and combine results
    for (int i = 0; i < nitems; i++)
    {
        bool isnull;
        Datum elt = array_iter_next(&iter, &isnull, i,
                                   typentry->typlen, typentry->typbyval, typentry->typalign);

        uint32 elthash;
        if (isnull) {
            elthash = 0;  // NULL elements have hash value 0
        } else {
            locfcinfo->args[0].value = elt;
            locfcinfo->args[0].isnull = false;
            elthash = DatumGetUInt32(FunctionCallInvoke(locfcinfo));
        }

        // Combine using multiplicative hash: result = (result * 31) + elthash
        result = (result << 5) - result + elthash;
    }

    AARR_FREE_IF_COPY(array, 0);
    PG_RETURN_UINT32(result);
}
```