# array_positions

## Location
[src/backend/utils/adt/array_userfuncs.c:1399-1536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1399-L1536)

## Overview
A PostgreSQL user function that returns an array of all positions where a specified element occurs in a one-dimensional array.

## Definition

```c
Datum
array_positions(PG_FUNCTION_ARGS)
```
## Detailed Description
 searches through a one-dimensional PostgreSQL array and returns an array containing the 1-based positions of all occurrences of a specified element. Unlike  which returns only the first match, this function finds and returns all matching positions. It uses "IS NOT DISTINCT FROM" semantics for comparisons, meaning it can properly handle NULL values in both the search element and array elements.

The function builds the result array incrementally using PostgreSQL's ArrayBuildState mechanism. When no matches are found, it returns an empty array (not NULL). For NULL input arrays, it returns NULL. The function maintains the same restrictions as other array position functions: it only works with one-dimensional arrays and will reject multi-dimensional arrays.

Like , it uses cached type information (ArrayMetaState) for efficiency across multiple calls and employs PostgreSQL's array iteration infrastructure for traversal.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to search within
  - : The element value to search for (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM, ARR_ELEMTYPE, ARR_LBOUND
  - [array_contains_nulls](array_contains_nulls.md)
  - [initArrayResult](../i/initArrayResult.md), accumArrayResult, makeArrayResult
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [array_create_iterator](array_create_iterator.md), array_iterate, array_free_iterator
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - SQL function  (through function catalog)

## Notes and Other Information
- Returns an empty array when no matches are found, unlike array_position which returns NULL
- Uses "IS NOT DISTINCT FROM" semantics for comparisons, properly handling NULL values
- Only supports one-dimensional arrays; throws ERRCODE_FEATURE_NOT_SUPPORTED for multi-dimensional arrays
- Maintains ArrayMetaState cache in fn_extra to optimize repeated calls with same element type
- Uses ArrayBuildState for efficient incremental array construction
- Memory management includes proper cleanup of toasted input arrays
- Returns integer array (INT4OID) containing all matching positions
- Can efficiently skip null searches when array contains no nulls
- Located in src/backend/utils/adt/array_userfuncs.c:1399-1536

## Simplified Source

```c
Datum
array_positions(PG_FUNCTION_ARGS)
{
    ArrayType  *array;
    Oid         element_type;
    Datum       searched_element, value;
    bool        isnull;
    int         position;
    bool        null_search;
    ArrayIterator array_iterator;
    ArrayBuildState *astate = NULL;
    ArrayMetaState *my_extra;
    TypeCacheEntry *typentry;

    // Return NULL if input array is NULL
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    array = PG_GETARG_ARRAYTYPE_P(0);

    // Only support 1-dimensional arrays
    if (ARR_NDIM(array) > 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("searching for elements in multidimensional arrays is not supported")));

    // Initialize result array builder
    astate = initArrayResult(INT4OID, CurrentMemoryContext, false);

    // Empty arrays return empty result array
    if (ARR_NDIM(array) < 1)
        PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));

    // Handle search element (NULL or actual value)
    if (PG_ARGISNULL(1)) {
        if (!array_contains_nulls(array))
            PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
        null_search = true;
    } else {
        searched_element = PG_GETARG_DATUM(1);
        null_search = false;
    }

    element_type = ARR_ELEMTYPE(array);
    position = (ARR_LBOUND(array))[0] - 1;

    // Cache type information for efficiency
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->element_type != element_type) {
        // Initialize or update cached type info
        if (my_extra == NULL) {
            fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                          sizeof(ArrayMetaState));
            my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        }

        get_typlenbyvalalign(element_type, &my_extra->typlen,
                           &my_extra->typbyval, &my_extra->typalign);

        typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

        if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("could not identify an equality operator for type %s",
                           format_type_be(element_type))));

        my_extra->element_type = element_type;
        fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
                      fcinfo->flinfo->fn_mcxt);
    }

    // Iterate through array and collect all matching positions
    array_iterator = array_create_iterator(array, 0, my_extra);
    while (array_iterate(array_iterator, &value, &isnull)) {
        position++;

        // Handle NULL searches and NULL elements
        if (isnull || null_search) {
            if (isnull && null_search) {
                astate = accumArrayResult(astate, Int32GetDatum(position), false,
                                        INT4OID, CurrentMemoryContext);
            }
            continue;
        }

        // Compare non-NULL values and add position if match found
        if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, PG_GET_COLLATION(),
                                         searched_element, value))) {
            astate = accumArrayResult(astate, Int32GetDatum(position), false,
                                    INT4OID, CurrentMemoryContext);
        }
    }

    array_free_iterator(array_iterator);
    PG_FREE_IF_COPY(array, 0);

    PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
}
```