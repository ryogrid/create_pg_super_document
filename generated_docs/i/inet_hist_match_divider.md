# inet_hist_match_divider

## Location
[src/backend/utils/adt/network_selfuncs.c:939-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L939-L972)

## Overview
Calculates a partial match divider value for inet histogram selectivity estimation, determining how many network bits distinguish a histogram boundary from a query value.

## Definition
static int inet_hist_match_divider(inet *boundary, inet *query, int opr_codenum)

## Detailed Description
This function computes a "divider" value used in histogram-based selectivity estimation for inet operators. The divider represents the number of non-common bits between a histogram boundary value and a query value, which helps estimate how much of a histogram bucket matches the query condition.

The calculation process:
1. Validates that both operands have the same IP family (IPv4/IPv6)
2. Checks that the mask length relationship satisfies the inclusion operator using inet_masklen_inclusion_cmp()
3. If valid, determines the "decisive" mask length based on the operator semantics
4. Calculates the number of common address bits up to the minimum mask length  
5. Returns the difference between the decisive mask length and the common bits

For supernet operators (negative codes), the boundary's mask length is decisive. For subnet operators (positive codes), the query's mask length is decisive. For overlap operators (code 0), the minimum of both mask lengths is used.

## Parameters / Member Variables
- : Pointer to the histogram boundary inet value for comparison
- : Pointer to the query inet value being evaluated
- : Numeric code representing the inclusion operator (from inet_opr_codenum)

## Dependencies
- Functions called/Symbols referenced:
  - ip_family (extracts IP family from inet value)
  - [inet_masklen_inclusion_cmp](inet_masklen_inclusion_cmp.md) (validates mask length relationship for operator)
  - ip_bits (extracts mask length from inet value)
  - Min (minimum macro)
  - [bitncommon](../b/bitncommon.md) (counts common leading bits between addresses)
  - ip_addr (extracts IP address portion from inet value)
- Called from (representative examples):
  - [inet_hist_value_sel](inet_hist_value_sel.md) (histogram-based selectivity estimation)

## Notes and Other Information
Returns -1 if the calculation cannot be performed (different IP families or incompatible mask length relationship). Otherwise returns a non-negative value representing the "distance" between the boundary and query in terms of network bits. A return value of 0 indicates an exact match, while positive values indicate the degree of mismatch. This value is used in the histogram interpolation logic to estimate what fraction of a histogram bucket satisfies the query condition.

## Simplified Source

```c
static int
inet_hist_match_divider(inet *boundary, inet *query, int opr_codenum)
{
    // Only process same IP families with compatible mask relationships
    if (ip_family(boundary) != ip_family(query) ||
        inet_masklen_inclusion_cmp(boundary, query, opr_codenum) != 0)
        return -1;

    int min_bits = Min(ip_bits(boundary), ip_bits(query));
    int decisive_bits;

    // Determine which mask length is "decisive" for the operator
    if (opr_codenum < 0)        // Supernet ops: boundary decisive
        decisive_bits = ip_bits(boundary);
    else if (opr_codenum > 0)   // Subnet ops: query decisive
        decisive_bits = ip_bits(query);
    else                        // Overlap op: minimum decisive
        decisive_bits = min_bits;

    // Calculate non-common decisive bits (0 = exact match, >0 = mismatch degree)
    if (min_bits > 0)
        return decisive_bits - bitncommon(ip_addr(boundary), ip_addr(query), min_bits);

    return decisive_bits;
}
```