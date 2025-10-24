# calc_inet_union_params_indexed

## Location
[src/backend/utils/adt/network_gist.c:407-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L407-L471)

## Overview
A static helper function that calculates union parameters for GistInetKey entries accessed through an indexed offset array, providing selective parameter calculation for non-contiguous key sets.

## Definition

```c
static void
calc_inet_union_params_indexed(GISTENTRY *ent,
							   OffsetNumber *offsets, int noffsets,
							   int *minfamily_p,
							   int *maxfamily_p,
							   int *minbits_p,
							   int *commonbits_p)
```
## Detailed Description
This function performs the same union parameter calculation as calc_inet_union_params() but operates on a non-contiguous subset of GISTENTRY elements specified by an array of offset indices. This selective approach is essential during GiST page splitting operations where keys need to be analyzed in specific groupings rather than contiguous ranges.

The function computes the same four union parameters:
1. **Family range**: Min/max IP address family numbers across selected keys
2. **Minimum netmask bits**: Smallest netmask width among selected keys
3. **Common address bits**: Leading address bits shared by all selected keys

Like its sibling function, it enforces the constraint that mixed address families result in zero minbits and commonbits values. The algorithm uses indirect indexing through the offsets array to access only the relevant GISTENTRY elements.

## Parameters / Member Variables
- `*ent`: Array of GISTENTRY elements containing inet keys
- `*offsets`: Array of OffsetNumber indices specifying which entries to examine
- `noffsets`: Number of elements in the offsets array
- `*minfamily_p`: Output parameter for minimum IP address family number
- `*maxfamily_p`: Output parameter for maximum IP address family number
- `*minbits_p`: Output parameter for minimum netmask width
- `*commonbits_p`: Output parameter for number of common address bits
## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInetKeyP
  - gk_ip_family, gk_ip_minbits, gk_ip_commonbits, gk_ip_addr
  - [bitncommon](../b/bitncommon.md)
  - Assert
  - OffsetNumber
- Called from (representative examples):
  - [inet_gist_picksplit](../i/inet_gist_picksplit.md) (multiple calls during page splitting)

## Notes and Other Information
- This is a static function, only accessible within network_gist.c
- Requires at least one offset (noffsets > 0 assertion)
- Uses indirect array indexing to access non-contiguous GISTENTRY elements
- Primarily used during GiST page splitting operations to analyze candidate key groupings
- The indexed approach allows flexible key set analysis without requiring data reorganization
- Maintains the same mixed-family constraint as the contiguous version

## Simplified Source

```c
static void
calc_inet_union_params_indexed(GISTENTRY *ent,
                                OffsetNumber *offsets, int noffsets,
                                int *minfamily_p, int *maxfamily_p,
                                int *minbits_p, int *commonbits_p)
{
    int minfamily, maxfamily, minbits, commonbits;
    unsigned char *addr;
    GistInetKey *tmp;

    // Initialize with first indexed key's values
    tmp = DatumGetInetKeyP(ent[offsets[0]].key);
    minfamily = maxfamily = gk_ip_family(tmp);
    minbits = gk_ip_minbits(tmp);
    commonbits = gk_ip_commonbits(tmp);
    addr = gk_ip_addr(tmp);

    // Scan remaining indexed keys to find ranges and commonality
    for (int i = 1; i < noffsets; i++)
    {
        tmp = DatumGetInetKeyP(ent[offsets[i]].key);

        // Track family range (IPv4/IPv6)
        if (minfamily > gk_ip_family(tmp))
            minfamily = gk_ip_family(tmp);
        if (maxfamily < gk_ip_family(tmp))
            maxfamily = gk_ip_family(tmp);

        // Find minimum netmask bits
        if (minbits > gk_ip_minbits(tmp))
            minbits = gk_ip_minbits(tmp);

        // Calculate common address bits
        if (commonbits > gk_ip_commonbits(tmp))
            commonbits = gk_ip_commonbits(tmp);
        if (commonbits > 0)
            commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
    }

    // Mixed families can't have meaningful bit commonality
    if (minfamily != maxfamily)
        minbits = commonbits = 0;

    // Return calculated parameters
    *minfamily_p = minfamily;
    *maxfamily_p = maxfamily;
    *minbits_p = minbits;
    *commonbits_p = commonbits;
}
```