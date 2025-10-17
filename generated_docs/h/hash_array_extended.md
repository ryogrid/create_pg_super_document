# hash_array_extended

## Location
[src/backend/utils/adt/arrayfuncs.c:4279-4368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4279-L4368)

## Overview
Enhanced array hashing function that produces 64-bit hash values with seed support for improved hash distribution and security in hash-based operations.

## Definition

```c
Datum
hash_array_extended(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the extended version of , designed to provide 64-bit hash values with seeded hashing capability. It follows the same basic algorithm as  but uses the extended hash functions for individual elements, which accept a 64-bit seed parameter to enhance hash distribution and provide protection against hash collision attacks.

Like its 32-bit counterpart, it uses the multiplicative hash algorithm  but operates on 64-bit values throughout. The function looks up extended hash functions via the type cache system and handles NULL elements by treating them as having hash value 0.

This function is particularly important for hash partitioning and other operations where high-quality hash distribution is critical and where protection against algorithmic complexity attacks is desired.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  -  - Get number of array dimensions
  -  - Get array dimension sizes
  -  - Get array element type OID
  -  - Get cached type information with extended hash function
  -  - Calculate total number of array elements
  -  - [Initialize](../I/Initialize.md) array iterator
  -  - Get next array element
  -  - Convert int64 to Datum for seed parameter
  -  - Call element extended hash function
  -  - Extract uint64 from Datum
  -  - Free detoasted array copies
  -  - Return 64-bit hash result

- Called from (representative examples):
  - Used as extended hash support function for hash indexes on array columns
  - Called by hash partitioning operations involving arrays
  - Used in hash-based operations requiring seed support

## Notes and Other Information
- Returns a 64-bit unsigned integer hash value for improved hash quality
- Requires element types to have extended hash functions available
- Seed parameter enhances hash distribution and provides security against hash flooding attacks
- Uses  flag to look up extended hash functions
- NULL elements are assigned hash value 0 for consistent behavior
- Does not handle RECORD types with special logic (unlike regular )
- [Hash](../H/Hash.md) algorithm provides better distribution properties due to 64-bit arithmetic
- Multiplicative constant 31 maintains good distribution properties in 64-bit space
- Essential for modern hash-based operations that require collision resistance

## Simplified Source

```c
Datum
hash_array_extended(PG_FUNCTION_ARGS)
{
    LOCAL_FCINFO(locfcinfo, 2);
    AnyArrayType *array = PG_GETARG_ANY_ARRAY_P(0);
    uint64 seed = PG_GETARG_INT64(1);

    // Extract array metadata
    int ndims = AARR_NDIM(array);
    int *dims = AARR_DIMS(array);
    Oid element_type = AARR_ELEMTYPE(array);
    uint64 result = 1;

    // Get cached extended hash function for element type
    TypeCacheEntry *typentry = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
    if (typentry == NULL || typentry->type_id != element_type)
    {
        typentry = lookup_type_cache(element_type, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        if (!OidIsValid(typentry->hash_extended_proc_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("could not identify an extended hash function for type %s",
                                  format_type_be(element_type))));
        fcinfo->flinfo->fn_extra = (void *) typentry;
    }

    // Setup element extended hashing
    InitFunctionCallInfoData(*locfcinfo, &typentry->hash_extended_proc_finfo, 2,
                            PG_GET_COLLATION(), NULL, NULL);

    int nitems = ArrayGetNItems(ndims, dims);
    array_iter iter;
    array_iter_setup(&iter, array);

    // Hash each element with seed and combine results
    for (int i = 0; i < nitems; i++)
    {
        bool isnull;
        Datum elt = array_iter_next(&iter, &isnull, i,
                                   typentry->typlen, typentry->typbyval, typentry->typalign);

        uint64 elthash;
        if (isnull) {
            elthash = 0;  // NULL elements have hash value 0
        } else {
            locfcinfo->args[0].value = elt;
            locfcinfo->args[0].isnull = false;
            locfcinfo->args[1].value = Int64GetDatum(seed);
            locfcinfo->args[1].isnull = false;
            elthash = DatumGetUInt64(FunctionCallInvoke(locfcinfo));
        }

        // Combine using multiplicative hash: result = (result * 31) + elthash
        result = (result << 5) - result + elthash;
    }

    AARR_FREE_IF_COPY(array, 0);
    PG_RETURN_UINT64(result);
}
```