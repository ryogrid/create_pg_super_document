# brin_minmax_multi_distance_inet

## Location
[src/backend/access/brin/brin_minmax_multi.c:2297-2379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2297-L2379)

## Overview
Computes the normalized distance between two inet values for BRIN minmax multi-column index operations, used for data type correlation analysis.

## Definition

```c
Datum
brin_minmax_multi_distance_inet(PG_FUNCTION_ARGS)
```
## Detailed Description
This function calculates the distance between two inet (IP address) values by converting the difference between their binary representations to a normalized floating-point value in the range [0,1]. The function is specifically designed for BRIN (Block Range Index) minmax multi-column operations where understanding data correlation is crucial for index effectiveness.

The distance calculation process:
1. Checks if both addresses belong to the same IP family (IPv4 or IPv6) - addresses from different families return maximum distance (1.0)
2. Applies network masks to both addresses based on their subnet mask lengths 
3. Computes byte-wise differences starting from the least significant byte
4. Normalizes the result by dividing by 256 for each byte position
5. Returns a value between 0.0 (identical addresses) and 1.0 (maximum distance within the same family)

The function handles both IPv4 (32-bit) and IPv6 (128-bit) addresses and considers subnet masks when calculating distances, though it operates on byte boundaries rather than individual bits.

## Parameters / Member Variables
- First parameter (PG_GETARG_INET_PP(0)): First inet value for distance calculation
- Second parameter (PG_GETARG_INET_PP(1)): Second inet value for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP: Extracts inet values from function arguments
  - ip_family: Determines IP address family (IPv4/IPv6)
  - ip_addr: Gets pointer to IP address bytes
  - ip_addrsize: Returns size of IP address in bytes
  - ip_bits: Gets subnet mask length in bits
  - [palloc](../p/palloc.md)/pfree: Memory allocation and deallocation
- Called from (representative examples):
  - Not directly referenced by other symbols in the codebase

## Notes and Other Information
- Returns 1.0 for addresses from different IP families (IPv4 vs IPv6)
- Currently ignores subnet mask bits when calculating distance (operates on byte boundaries)  
- The function includes an assertion that the result is always between 0 and 1
- Memory is allocated for address copying and properly freed after calculation
- Part of the BRIN minmax multi-column index infrastructure for optimizing range queries on correlated data

## Simplified Source

```c
Datum brin_minmax_multi_distance_inet(PG_FUNCTION_ARGS) {
    inet *ipa = PG_GETARG_INET_PP(0);
    inet *ipb = PG_GETARG_INET_PP(1);

    // Different IP families are at maximum distance
    if (ip_family(ipa) != ip_family(ipb)) {
        return PG_RETURN_FLOAT8(1.0);
    }

    // Copy address bytes to apply masks
    int len = ip_addrsize(ipa);
    unsigned char *addra = (unsigned char *) palloc(len);
    unsigned char *addrb = (unsigned char *) palloc(len);
    memcpy(addra, ip_addr(ipa), len);
    memcpy(addrb, ip_addr(ipb), len);

    // Apply network masks to both addresses
    int lena = ip_bits(ipa);
    int lenb = ip_bits(ipb);
    for (int i = 0; i < len; i++) {
        // Mask bits for first address
        int nbits = Max(0, lena - (i * 8));
        if (nbits < 8) {
            unsigned char mask = (0xFF << (8 - nbits));
            addra[i] = (addra[i] & mask);
        }

        // Mask bits for second address
        nbits = Max(0, lenb - (i * 8));
        if (nbits < 8) {
            unsigned char mask = (0xFF << (8 - nbits));
            addrb[i] = (addrb[i] & mask);
        }
    }

    // Calculate normalized distance (base-256 from least significant byte)
    float8 delta = 0;
    for (int i = len - 1; i >= 0; i--) {
        delta += (float8) addrb[i] - (float8) addra[i];
        delta /= 256;
    }

    pfree(addra);
    pfree(addrb);

    return PG_RETURN_FLOAT8(delta);
}
```