# network_cmp_internal

## Location
[src/backend/utils/adt/network.c:405-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L405-L424)

## Overview
Core comparison function that implements hierarchical network address comparison logic for sorting and inet/cidr operations.

## Definition

```c
static int32
network_cmp_internal(inet *a1, inet *a2)
```
## Detailed Description
This function provides the fundamental comparison logic for network addresses in PostgreSQL. It implements a three-tier comparison algorithm designed specifically for network operations:

1. **Common network bits comparison**: First compares the network portions using the shorter of the two mask lengths
2. **Mask length comparison**: If network portions are equal, compares the mask lengths themselves
3. **Full address comparison**: If both network parts and mask lengths are equal, performs a full address comparison

The comparison logic ensures that network addresses are sorted in a hierarchical manner where the network part is the primary sort key, followed by mask specificity, and finally the complete address. This ordering is essential for CIDR operations and requires that address bits beyond the mask are properly zeroed.

For different address families (IPv4 vs IPv6), the function simply compares the family values to establish ordering.

## Parameters / Member Variables
- `*a1`: First network address ( structure) to compare
- `*a2`: Second network address ( structure) to compare
## Dependencies
- Functions called/Symbols referenced:
  -  (address family accessor)
  -  (bit-wise comparison function)
  -  (address bytes accessor)
  -  (mask length accessor)
  -  (maximum bits for family)
  -  (minimum value macro)

- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- Returns negative value if a1 < a2, zero if equal, positive if a1 > a2
- The comparison algorithm is specifically designed for CIDR operations where logical equality requires proper bit masking
- Critical for sort support operations and B-tree indexing of network types
- The function assumes that CIDR addresses have properly zeroed bits beyond their mask length for consistent comparison results
- Located in src/backend/utils/adt/network.c:405-424
- Marked as static - internal function not exposed outside the network.c compilation unit

## Simplified Source

```c
static int32
network_cmp_internal(inet *a1, inet *a2)
{
    // Different address families: compare by family type
    if (ip_family(a1) != ip_family(a2))
        return ip_family(a1) - ip_family(a2);

    // Same address family: hierarchical comparison
    int order;

    // 1. Compare common network bits (using shorter mask)
    order = bitncmp(ip_addr(a1), ip_addr(a2),
                    Min(ip_bits(a1), ip_bits(a2)));
    if (order != 0)
        return order;

    // 2. Compare mask lengths
    order = ((int) ip_bits(a1)) - ((int) ip_bits(a2));
    if (order != 0)
        return order;

    // 3. Compare full addresses as tiebreaker
    return bitncmp(ip_addr(a1), ip_addr(a2), ip_maxbits(a1));
}
```