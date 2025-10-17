# array_contain_compare

## Location
[src/backend/utils/adt/arrayfuncs.c:4369-4511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4369-L4511)

## Overview
Internal function that implements array overlap and containment comparisons by checking if elements from one array exist in another array based on configurable matching criteria.

## Definition

```c
struct_array on it.  We scan array1 the hard way
	 * however, since we very likely won't need to look at all of it.
	 */
	if (VARATT_IS_EXPANDED_HEADER(array2))
	{
		/* This should be safe even if input is read-only */
		deconstruct_expanded_array(&(array2->xpn));
		values2 = array2->xpn.dvalues;
		nulls2 = array2->xpn.dnulls;
		nelems2 = array2->xpn.nelems;
	}
	else
		deconstruct_array((ArrayType *) array2,
						  element_type, typlen, typbyval, typalign,
						  &values2, &nulls2, &nelems2);
```
## Detailed Description
The  function provides the core logic for array overlap and containment operations. It compares two arrays element-by-element to determine either overlap (any elements in common) or containment (all elements of one array exist in another), depending on the  parameter.

When  is true, the function returns true only if all elements of array1 are found in array2 (containment). When  is false, it returns true if any element of array1 is found in array2 (overlap). The function optimizes performance by deconstructing array2 into separate values and nulls arrays for efficient multiple scans, while iterating through array1 using the array iterator interface.

The function handles NULL values by treating them as non-matchable - NULL elements cannot match anything, including other NULLs, which differs from the behavior in .

## Parameters / Member Variables
- `array1`: First array to compare (source array for containment/overlap check)
- `array2`: Second array to compare (target array to search within)
- `collation`: Collation OID for element comparisons
- `matchall`: Boolean flag controlling comparison mode:
  - `true`: All elements of array1 must be in array2 (containment)
  - `false`: Any element of array1 must be in array2 (overlap)
- `typentry`: Pointer to cached type information for performance optimization

## Dependencies
- Functions called/Symbols referenced:
  -  - Get array element type
  -  - Get cached equality operator information
  -  - Check if array is in expanded format
  -  - Extract elements from expanded array
  -  - Extract elements from regular array format
  -  - Calculate total number of elements in array1
  -  /  - Get array dimensions for element count
  -  - [Initialize](../I/Initialize.md) iterator for array1
  -  - Get next element from array1
  -  - Set up function call for equality operator
  -  - Call equality operator on element pairs
  -  - Extract boolean result from equality comparison

- Called from (representative examples):
  -  - Array overlap operator (&& operator)
  -  - Array contains operator (@> operator) 
  -  - Array contained by operator (<@ operator)

## Notes and Other Information
- Returns boolean result indicating whether the containment/overlap condition is met
- Requires arrays to have the same element type; raises error for type mismatches
- Uses type cache to avoid repeated equality operator lookups for performance
- Optimizes array2 access by deconstructing it once into values/nulls arrays
- Handles expanded array format efficiently for better performance with large arrays
- NULL elements are treated as non-matchable (different from array equality semantics)
- Uses strict equality operators, so comparison results are never NULL
- Performance is O(n*m) where n and m are the number of elements in each array
- Early termination when result can be determined (first match for overlap, first non-match for containment)

## Simplified Source

```c
static bool
array_contain_compare(AnyArrayType *array1, AnyArrayType *array2, Oid collation,
                      bool matchall, void **fn_extra)
{
    LOCAL_FCINFO(locfcinfo, 2);
    bool result = matchall;  // Start with true for containment, false for overlap
    Oid element_type = AARR_ELEMTYPE(array1);

    // Validate element types match
    if (element_type != AARR_ELEMTYPE(array2))
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                       errmsg("cannot compare arrays of different element types")));

    // Get cached equality operator for element type
    TypeCacheEntry *typentry = (TypeCacheEntry *) *fn_extra;
    if (typentry == NULL || typentry->type_id != element_type)
    {
        typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);
        if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("could not identify an equality operator for type %s",
                                  format_type_be(element_type))));
        *fn_extra = (void *) typentry;
    }

    // Deconstruct array2 for efficient multiple scans
    Datum *values2;
    bool *nulls2;
    int nelems2;
    if (VARATT_IS_EXPANDED_HEADER(array2))
    {
        deconstruct_expanded_array(&(array2->xpn));
        values2 = array2->xpn.dvalues;
        nulls2 = array2->xpn.dnulls;
        nelems2 = array2->xpn.nelems;
    }
    else
    {
        deconstruct_array((ArrayType *) array2, element_type,
                         typentry->typlen, typentry->typbyval, typentry->typalign,
                         &values2, &nulls2, &nelems2);
    }

    // Setup equality comparison function
    InitFunctionCallInfoData(*locfcinfo, &typentry->eq_opr_finfo, 2,
                            collation, NULL, NULL);

    // Iterate through array1 elements
    int nelems1 = ArrayGetNItems(AARR_NDIM(array1), AARR_DIMS(array1));
    array_iter it1;
    array_iter_setup(&it1, array1);

    for (int i = 0; i < nelems1; i++)
    {
        bool isnull1;
        Datum elt1 = array_iter_next(&it1, &isnull1, i,
                                    typentry->typlen, typentry->typbyval, typentry->typalign);

        // NULL elements can't match anything
        if (isnull1)
        {
            if (matchall) {
                result = false;
                break;
            }
            continue;
        }

        // Search for elt1 in array2
        bool found = false;
        for (int j = 0; j < nelems2; j++)
        {
            bool isnull2 = nulls2 ? nulls2[j] : false;
            if (isnull2)
                continue;  // Skip NULL elements

            // Compare elements using equality operator
            locfcinfo->args[0].value = elt1;
            locfcinfo->args[0].isnull = false;
            locfcinfo->args[1].value = values2[j];
            locfcinfo->args[1].isnull = false;
            locfcinfo->isnull = false;

            bool oprresult = DatumGetBool(FunctionCallInvoke(locfcinfo));
            if (!locfcinfo->isnull && oprresult)
            {
                found = true;
                break;
            }
        }

        if (found)
        {
            if (!matchall) {
                result = true;  // Found overlap
                break;
            }
        }
        else
        {
            if (matchall) {
                result = false; // Missing element for containment
                break;
            }
        }
    }

    return result;
}
```