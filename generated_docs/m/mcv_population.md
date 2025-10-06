# mcv_population

## Location
[src/backend/utils/adt/network_selfuncs.c:539-603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L539-L603)

## Overview
Calculates the total fraction of a relation's population that is represented by the Most Common Values (MCV) statistics array.

## Definition
```c
static Selectivity mcv_population(float4 *mcv_numbers, int mcv_nvalues)
```

## Detailed Description
The `mcv_population` function is a utility that computes the cumulative population fraction covered by all entries in a Most Common Values (MCV) frequency array. It simply iterates through the array of frequencies and sums them to determine what proportion of the total relation population is represented by the most common values.

This calculation is essential for selectivity estimation algorithms that need to distinguish between the population covered by MCV statistics versus the remaining population that would be covered by histogram statistics. The result helps determine proper scaling factors when combining MCV-based selectivity with histogram-based selectivity.

## Parameters / Member Variables
- `mcv_numbers`: Array of frequency values for each most common value (as float4)
- `mcv_nvalues`: Number of elements in the MCV frequency array

## Dependencies
- Functions called/Symbols referenced:
  - float4 (data type)
- Called from (representative examples):
  - [networkjoinsel_inner](../n/networkjoinsel_inner.md)
  - [networkjoinsel_semi](../n/networkjoinsel_semi.md)

## Notes and Other Information
- Returns a Selectivity value (typically between 0.0 and 1.0) representing the population fraction
- Used to calculate the complement fraction (1.0 - sumcommon) for histogram-based populations  
- Simple summation algorithm with O(n) complexity where n is the number of MCV entries
- Critical for proper weighting in multi-source selectivity estimation (MCV + histogram)
- Helps prevent double-counting when combining statistics from different sources
- Function is static (internal to network_selfuncs.c) as it's a utility for network selectivity functions

## Simplified Source

```c
static Selectivity
mcv_population(float4 *mcv_numbers, int mcv_nvalues)
{
    Selectivity sumcommon = 0.0;

    // Sum all MCV frequency values to get total population fraction
    for (int i = 0; i < mcv_nvalues; i++)
    {
        sumcommon += mcv_numbers[i];
    }

    return sumcommon;
}
```