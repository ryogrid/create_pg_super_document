# array_position_common

## Location
[src/backend/utils/adt/array_userfuncs.c:1244-1398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_userfuncs.c#L1244-L1398)

## Overview
Common implementation function that searches for an element within a one-dimensional PostgreSQL array and returns its position.

## Definition

```c
static Datum
array_position_common(FunctionCallInfo fcinfo)
```
## Detailed Description
 is the core implementation for both  and  PostgreSQL functions. It searches for a specified element within a one-dimensional array and returns the 1-based index of the first occurrence found at or after a specified starting position. The function handles both null and non-null search elements, uses cached type information for efficiency across multiple calls, and employs PostgreSQL's array iteration infrastructure.

The function performs several validation checks: it rejects multi-dimensional arrays (since reporting element location would be ambiguous), handles empty arrays by returning NULL, and validates that starting positions are not null when provided. For null element searches, it can quickly return NULL if the array contains no nulls.

The search implementation uses PostgreSQL's ArrayIterator for efficient traversal and maintains type cache information (ArrayMetaState) in the function's local context to avoid repeated type lookups. It uses the element type's equality operator to compare values during the search.

## Parameters / Member Variables
- : Function call information structure containing:
  - : The input array to search within
  - : The element value to search for (can be NULL)
  -  (optional): Starting position for the search (1-based)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GET_COLLATION
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM, ARR_ELEMTYPE, ARR_LBOUND
  - [array_contains_nulls](array_contains_nulls.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [array_create_iterator](array_create_iterator.md), array_iterate, array_free_iterator
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - [array_position](array_position.md)
  - [array_position_start](array_position_start.md)

## Notes and Other Information
- Static function shared between array_position and array_position_start wrapper functions
- Maintains ArrayMetaState cache in fn_extra to optimize repeated calls with same element type
- Only supports one-dimensional arrays; throws ERRCODE_FEATURE_NOT_SUPPORTED for multi-dimensional arrays
- Returns NULL for empty arrays, not found elements, or NULL input arrays
- Handles NULL element searches efficiently by checking array_contains_nulls first
- Uses collation-aware comparison through FunctionCall2Coll for proper element matching
- Memory management includes proper cleanup of toasted input arrays
- Located in src/backend/utils/adt/array_userfuncs.c:1244-1398

## Simplified Source

```c
static Datum
array_position_common(FunctionCallInfo fcinfo)
{
    ArrayType  *array;
    Oid         element_type;
    Datum       searched_element;
    Datum       value;
    bool        isnull;
    int         position, position_min;
    bool        found = false;
    bool        null_search;
    ArrayIterator array_iterator;
    ArrayMetaState *my_extra;
    TypeCacheEntry *typentry;

    // Return NULL if array is NULL
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    array = PG_GETARG_ARRAYTYPE_P(0);

    // Only support 1-dimensional arrays
    if (ARR_NDIM(array) > 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("searching for elements in multidimensional arrays is not supported")));

    // Empty arrays always return NULL
    if (ARR_NDIM(array) < 1)
        PG_RETURN_NULL();

    // Handle search element (NULL or actual value)
    if (PG_ARGISNULL(1)) {
        if (!array_contains_nulls(array))
            PG_RETURN_NULL();
        null_search = true;
    } else {
        searched_element = PG_GETARG_DATUM(1);
        null_search = false;
    }

    element_type = ARR_ELEMTYPE(array);
    position = (ARR_LBOUND(array))[0] - 1;

    // Set starting position (optional 3rd argument)
    if (PG_NARGS() == 3) {
        if (PG_ARGISNULL(2))
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("initial position must not be null")));
        position_min = PG_GETARG_INT32(2);
    } else {
        position_min = (ARR_LBOUND(array))[0];
    }

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

    // Iterate through array elements
    array_iterator = array_create_iterator(array, 0, my_extra);
    while (array_iterate(array_iterator, &value, &isnull)) {
        position++;

        // Skip elements before starting position
        if (position < position_min)
            continue;

        // Handle NULL searches and NULL elements
        if (isnull || null_search) {
            if (isnull && null_search) {
                found = true;
                break;
            } else {
                continue;
            }
        }

        // Compare non-NULL values using equality operator
        if (DatumGetBool(FunctionCall2Coll(&my_extra->proc, PG_GET_COLLATION(),
                                         searched_element, value))) {
            found = true;
            break;
        }
    }

    array_free_iterator(array_iterator);
    PG_FREE_IF_COPY(array, 0);

    if (!found)
        PG_RETURN_NULL();

    PG_RETURN_INT32(position);
}
```