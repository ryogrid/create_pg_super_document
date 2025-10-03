# mcelem_array_contain_overlap_selec

## Location
[src/backend/utils/adt/array_selfuncs.c:521-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L521-L695)

## Overview
Estimates selectivity for array containment (@>) and overlap (&&) operators based on most common element statistics, assuming independent element occurrences.

## Definition

```c
static Selectivity
mcelem_array_contain_overlap_selec(Datum *mcelem, int nmcelem,
								   float4 *numbers, int nnumbers,
								   Datum *array_data, int nitems,
								   Oid operator, TypeCacheEntry *typentry)
```
## Detailed Description
This function calculates selectivity estimates for array containment and overlap operations by analyzing the most common elements (MCELEM) statistics from the array column. It processes each element in the constant array and computes the probability that rows will satisfy the given operator condition.

For containment (@>), it starts with selectivity 1.0 and multiplies by each element's selectivity (intersection probability). For overlap (&&), it starts with selectivity 0.0 and uses the union probability formula: selec = selec + elem_selec - selec * elem_selec.

The function uses either binary search or linear scan to find matching elements in the MCELEM array, depending on which approach is more efficient based on the array sizes.

## Parameters / Member Variables
- `*mcelem`: Array of most common element values from statistics (presorted)
- `nmcelem`: Number of elements in mcelem array
- `*numbers`: Array of frequency values corresponding to mcelem elements
- `nnumbers`: Number of elements in numbers array (should be nmcelem + 3)
- `*array_data`: Elements from the constant array being compared (presorted)
- `nitems`: Number of elements in array_data
- `operator`: The array operator being used (OID_ARRAY_CONTAINS_OP for @>, others for &&)
- `*typentry`: Type cache entry for element comparison functions
## Dependencies
- Functions called/Symbols referenced:
  - [floor_log2](../f/floor_log2.md)
  - [element_compare](../e/element_compare.md)
  - [find_next_mcelem](../f/find_next_mcelem.md)
  - DEFAULT_CONTAIN_SEL
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [scalararraysel_containment](../s/scalararraysel_containment.md)
  - [mcelem_array_selec](mcelem_array_selec.md)

## Notes and Other Information
- Assumes element occurrences are independent, which may not always be accurate in practice
- Uses binary search optimization when nitems * floor_log2(nmcelem) < nmcelem + nitems
- Falls back to default selectivity estimates when elements are not found in MCELEM statistics
- TODO comment suggests potential improvement by using distinct elements count histogram
- The function expects numbers array to have exactly nmcelem + 3 elements (frequencies + min/max/null stats)

## Simplified Source

```c
static Selectivity
mcelem_array_contain_overlap_selec(Datum *mcelem, int nmcelem,
                                   float4 *numbers, int nnumbers,
                                   Datum *array_data, int nitems,
                                   Oid operator, TypeCacheEntry *typentry)
{
    Selectivity selec, elem_selec;
    int mcelem_index, i;
    bool use_bsearch;
    float4 minfreq;

    // Validate statistics format - need nmcelem + 3 numbers
    if (nnumbers != nmcelem + 3) {
        numbers = NULL;
        nnumbers = 0;
    }

    // Get minimum frequency from stats or use default
    minfreq = numbers ? numbers[nmcelem] : 2 * DEFAULT_CONTAIN_SEL;

    // Choose search strategy based on efficiency
    use_bsearch = (nitems * floor_log2(nmcelem) < nmcelem + nitems);

    // Initialize selectivity based on operator type
    if (operator == OID_ARRAY_CONTAINS_OP)
        selec = 1.0;  // Start with 1.0 for containment (@>)
    else
        selec = 0.0;  // Start with 0.0 for overlap (&&)

    // Process each element in the constant array
    mcelem_index = 0;
    for (i = 0; i < nitems; i++) {
        bool match = false;

        // Skip duplicate elements
        if (i > 0 && element_compare(&array_data[i-1], &array_data[i], typentry) == 0)
            continue;

        // Find matching element in MCELEM statistics
        if (use_bsearch) {
            match = find_next_mcelem(mcelem, nmcelem, array_data[i],
                                   &mcelem_index, typentry);
        } else {
            // Linear search through mcelem array
            while (mcelem_index < nmcelem) {
                int cmp = element_compare(&mcelem[mcelem_index], &array_data[i], typentry);
                if (cmp < 0)
                    mcelem_index++;
                else {
                    if (cmp == 0) match = true;
                    break;
                }
            }
        }

        // Calculate element selectivity
        if (match && numbers) {
            elem_selec = numbers[mcelem_index];  // Use actual frequency
            mcelem_index++;
        } else {
            elem_selec = Min(DEFAULT_CONTAIN_SEL, minfreq / 2);  // Estimate for unknown elements
        }

        // Update overall selectivity using independence assumption
        if (operator == OID_ARRAY_CONTAINS_OP)
            selec *= elem_selec;  // Intersection: P(A and B) = P(A) * P(B)
        else
            selec = selec + elem_selec - selec * elem_selec;  // Union: P(A or B) = P(A) + P(B) - P(A)*P(B)

        CLAMP_PROBABILITY(selec);
    }

    return selec;
}
```