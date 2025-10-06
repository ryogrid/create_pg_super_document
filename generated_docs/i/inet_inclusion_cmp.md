# inet_inclusion_cmp

## Location
[src/backend/utils/adt/network_selfuncs.c:879-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L879-L904)

## Overview
A comparison function for subnet inclusion and overlap operators that determines the relative ordering of two inet values based on network inclusion semantics.

## Definition
static int inet_inclusion_cmp(inet *left, inet *right, int opr_codenum)

## Detailed Description
This function implements a specialized comparison for inet network types that supports subnet inclusion and overlap operations. It performs a two-stage comparison process:

1. First compares the common bits of the network portion up to the minimum mask length of both operands
2. If the network portions are identical, delegates to inet_masklen_inclusion_cmp() for mask length comparison that considers the specific inclusion operator

The function is compatible with the basic inet comparison semantics from network_cmp_internal() but is specifically designed to support inclusion operators. For different IP families (IPv4 vs IPv6), it falls back to simple family comparison.

The return value follows standard comparison conventions: 0 indicates the comparison is satisfied for the specified operator, while negative/positive values indicate the relative ordering for unsatisfied comparisons.

## Parameters / Member Variables
- : Pointer to the left inet operand for comparison
- : Pointer to the right inet operand for comparison  
- : Numeric code representing the specific inclusion operator (from inet_opr_codenum)

## Dependencies
- Functions called/Symbols referenced:
  - ip_family (extracts IP family from inet value)
  - [bitncmp](../b/bitncmp.md) (bit-wise comparison of network addresses)
  - ip_addr (extracts IP address portion from inet value)
  - ip_bits (extracts mask length from inet value)  
  - Min (minimum macro)
  - [inet_masklen_inclusion_cmp](inet_masklen_inclusion_cmp.md) (mask length comparison with operator awareness)
- Called from (representative examples):
  - [inet_hist_value_sel](inet_hist_value_sel.md) (histogram-based selectivity estimation)

## Notes and Other Information
The function separates network address comparison from mask length comparison for modularity and reusability. The first stage uses bitncmp() to compare only the common network bits (up to the shorter mask length), ensuring that different mask lengths don't affect the network portion comparison. Only when network portions are identical does it proceed to the operator-specific mask length comparison via inet_masklen_inclusion_cmp().

## Simplified Source

```c
static int
inet_inclusion_cmp(inet *left, inet *right, int opr_codenum)
{
    // Different IP families (IPv4 vs IPv6) - compare family numbers
    if (ip_family(left) != ip_family(right))
        return ip_family(left) - ip_family(right);

    // Same family - compare network bits up to minimum mask length
    int order = bitncmp(ip_addr(left), ip_addr(right),
                        Min(ip_bits(left), ip_bits(right)));

    // If network portions differ, return that difference
    if (order != 0)
        return order;

    // Network portions identical - compare mask lengths with operator awareness
    return inet_masklen_inclusion_cmp(left, right, opr_codenum);
}
```