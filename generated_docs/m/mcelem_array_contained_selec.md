# mcelem_array_contained_selec

## Location
[src/backend/utils/adt/array_selfuncs.c:696-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L696-L852)

## Overview
Estimates selectivity for array contained-by (<@) operator based on most common element statistics and distinct element count histogram, correcting for element occurrence dependencies.

## Definition
```c
static Selectivity mcelem_array_contained_selec(Datum *mcelem, int nmcelem,
                                               float4 *numbers, int nnumbers,
                                               Datum *array_data, int nitems,
                                               float4 *hist, int nhist,
                                               Oid operator, TypeCacheEntry *typentry)
```

## Detailed Description
This function calculates selectivity estimates for the array contained-by (<@) operator by analyzing most common elements and using a histogram of distinct element counts to correct for element occurrence dependencies. Unlike simple containment/overlap operations, contained-by queries typically involve arrays with many elements, making the effect of element dependencies significant.

The function uses a sophisticated probability model that accounts for the distribution of distinct element counts in the data, rather than assuming independent element occurrences. It implements the formula:

P(o1, o2, ..., on) = f1^o1 * (1-f1)^(1-o1) * ... * fn^on * (1-fn)^(1-on) * hist[m] / ind[m]

where oi represents element occurrences, fi represents element frequencies, m is the total distinct elements, hist[m] is histogram data, and ind[m] is the independent probability.

## Parameters / Member Variables
- `mcelem`: Array of most common element values from statistics (presorted)
- `nmcelem`: Number of elements in mcelem array  
- `numbers`: Array of frequency values corresponding to mcelem elements
- `nnumbers`: Number of elements in numbers array (must be nmcelem + 3)
- `array_data`: Elements from the constant array being compared (presorted)
- `nitems`: Number of elements in array_data
- `hist`: Histogram of distinct element counts from DECHIST statistics slot
- `nhist`: Number of histogram buckets (must be >= 3)
- `operator`: The array operator being used (for contained-by operations)
- `typentry`: Type cache entry for element comparison functions

## Dependencies
- Functions called/Symbols referenced:
  - [element_compare](../e/element_compare.md)
  - DEFAULT_CONTAIN_SEL
  - [palloc](../p/palloc.md)
  - exp (math function)
  - [calc_distr](../c/calc_distr.md)
  - [calc_hist](../c/calc_hist.md)
- Called from (representative examples):
  - [scalararraysel_containment](../s/scalararraysel_containment.md)
  - [mcelem_array_selec](mcelem_array_selec.md)

## Notes and Other Information
- Requires both MCELEM and DECHIST statistics to function properly; falls back to DEFAULT_CONTAIN_SEL if unavailable
- Uses Poisson distribution to model rare element occurrences: mult *= exp(-rest)
- More sophisticated than simple containment/overlap because contained-by typically involves larger constant arrays
- Accounts for element occurrence dependencies that are common in real datasets
- The algorithm processes elements in sorted order and maintains running calculations of probability multipliers