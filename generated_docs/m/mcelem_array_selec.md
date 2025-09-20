# mcelem_array_selec

## Location
[src/backend/utils/adt/array_selfuncs.c:428-520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L428-L520)

## Overview
Fundamental array selectivity estimation function that processes constant arrays and delegates to specialized selectivity calculation functions based on the containment operator type.

## Definition

```c
struct_array(array,
					  typentry->type_id,
					  typentry->typlen,
					  typentry->typbyval,
					  typentry->typalign,
					  &elem_values, &elem_nulls, &num_elems);
```
## Detailed Description
This function serves as the central dispatcher for array selectivity estimation using most-common-elements (MCE) statistics. It prepares the constant array by deconstructing it into individual elements, removing nulls, and sorting the elements for efficient comparison with statistical data.

The function handles special cases for null elements:
- For  (contains): if the constant array contains null, selectivity is 0 (nothing matches)
- For  (overlaps) and  (contained by): null elements are ignored

After preprocessing, it delegates to specialized functions:
- mcelem_array_contain_overlap_selec for  and  operations
- mcelem_array_contained_selec for  operations

## Parameters
- : ArrayType pointer to the constant array value
- : Type cache entry containing element type information  
- : Array of most-common-element values from statistics
- : Number of most-common-elements
- : Frequency values corresponding to most-common-elements
- : Number of frequency values
- : Histogram of distinct-element counts (used for <@ operator)
- : Number of histogram values
- : OID of the containment operator

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array](../d/deconstruct_array.md)
  - qsort_arg with element_compare
  - [mcelem_array_contain_overlap_selec](mcelem_array_contain_overlap_selec.md)
  - [mcelem_array_contained_selec](mcelem_array_contained_selec.md)
- Called from:
  - [calc_arraycontsel](../c/calc_arraycontsel.md) (multiple calls at lines 382, 394, 407)

## Notes and Other Information
- Static function (internal to array_selfuncs.c)
- Handles memory management by freeing allocated arrays
- Implements special null-handling logic for different operators
- Sorts constant array elements for efficient statistical analysis
- Validates operator type and raises ERROR for unrecognized operators
- Core component of PostgreSQL's array selectivity estimation framework