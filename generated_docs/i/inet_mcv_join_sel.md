# inet_mcv_join_sel

## Location
[src/backend/utils/adt/network_selfuncs.c:673-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L673-L704)

## Overview
Estimates join selectivity between two Most Common Values (MCV) lists for inet network operations by computing the exact fraction of populations that satisfy the join clause.

## Definition

```c
static Selectivity
inet_mcv_join_sel(Datum *mcv1_values, float4 *mcv1_numbers, int mcv1_nvalues,
				  Datum *mcv2_values, float4 *mcv2_numbers, int mcv2_nvalues,
				  Oid operator)
```
## Detailed Description
This function performs selectivity estimation for join operations between two relations based on their Most Common Values (MCV) statistics. It uses a straightforward approach by testing every combination of values from both MCV lists using the specified operator.

For each pair of values (one from each MCV list), the function applies the join operator. If the operator returns true for a pair, it contributes to the total selectivity by multiplying the frequencies of both values (mcv1_numbers[i] * mcv2_numbers[j]). This approach produces an exact result that doesn't require further scaling.

The function is particularly useful for join selectivity estimation in network operations where MCV statistics are available for both sides of the join, providing more accurate estimates than histogram-based methods for the most frequently occurring values.

## Parameters / Member Variables
- `*mcv1_values`: Array of Datum values from the first relation's MCV list
- `*mcv1_numbers`: Array of frequency values corresponding to mcv1_values
- `mcv1_nvalues`: Number of entries in the first MCV list
- `*mcv2_values`: Array of Datum values from the second relation's MCV list
- `*mcv2_numbers`: Array of frequency values corresponding to mcv2_values
- `mcv2_nvalues`: Number of entries in the second MCV list
- `operator`: OID of the join operator to apply
## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md)
  - [fmgr_info](../f/fmgr_info.md)
  - FunctionCall2
  - [DatumGetBool](../D/DatumGetBool.md)
- Called from (representative examples):
  - [networkjoinsel_inner](../n/networkjoinsel_inner.md)

## Notes and Other Information
- Returns exact selectivity without requiring scaling, unlike histogram-based estimates
- Time complexity is O(mcv1_nvalues × mcv2_nvalues), so it's efficient for typical MCV list sizes
- Works with any inet comparison operator by dynamically looking up the operator function
- The result represents the probability that a random tuple pair from both relations will satisfy the join condition
- Most effective when both relations have good MCV coverage for their inet columns

## Simplified Source

```c
static Selectivity
inet_mcv_join_sel(Datum *mcv1_values, float4 *mcv1_numbers, int mcv1_nvalues,
                  Datum *mcv2_values, float4 *mcv2_numbers, int mcv2_nvalues,
                  Oid operator)
{
    Selectivity selec = 0.0;
    FmgrInfo proc;

    // Get operator function information
    fmgr_info(get_opcode(operator), &proc);

    // Test every combination of MCV values from both relations
    for (int i = 0; i < mcv1_nvalues; i++)
    {
        for (int j = 0; j < mcv2_nvalues; j++)
        {
            // Apply operator to current value pair
            if (DatumGetBool(FunctionCall2(&proc, mcv1_values[i], mcv2_values[j])))
            {
                // If operator returns true, add product of frequencies
                selec += mcv1_numbers[i] * mcv2_numbers[j];
            }
        }
    }

    return selec;
}
```