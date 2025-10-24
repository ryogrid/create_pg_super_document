# inet_merge

## Location
[src/backend/utils/adt/network.c:1476-1501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1476-L1501)

## Overview
Computes the smallest CIDR network that contains both of the input inet addresses.

## Definition
```c
Datum inet_merge(PG_FUNCTION_ARGS)
```

## Detailed Description
This function takes two inet addresses and returns the smallest CIDR (Classless Inter-Domain Routing) network that encompasses both addresses. It first validates that both addresses belong to the same address family (IPv4 or IPv6), then calculates the number of common leading bits between the two addresses. The result is a new CIDR network with the mask length set to the number of common bits, effectively creating the minimal supernet that contains both input addresses.

## Parameters / Member Variables
- First argument: inet address (accessed via `PG_GETARG_INET_PP(0)`)
- Second argument: inet address (accessed via `PG_GETARG_INET_PP(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INET_PP` - macro to extract inet arguments
  - `ip_family` - extracts the address family from inet structure
  - [bitncommon](../b/bitncommon.md) - calculates common leading bits between two addresses
  - `ip_addr` - extracts the address portion from inet structure
  - `ip_bits` - extracts the mask length from inet structure
  - [cidr_set_masklen_internal](../c/cidr_set_masklen_internal.md) - creates new CIDR with specified mask length
  - `PG_RETURN_INET_P` - macro to return inet result
  - `ereport` - [error](../e/error.md) reporting function
- Called from (representative examples):
  - Not directly referenced by other functions (likely used through SQL function calls)

## Notes and Other Information
- Throws an error if the input addresses are from different address families
- The resulting CIDR mask length is determined by the number of common leading bits
- Essential for network aggregation and supernet calculation operations
- Located in src/backend/utils/adt/network.c:1476-1501
- Uses the Min() macro to ensure the comparison doesn't exceed the smaller of the two input mask lengths

## Simplified Source

```c
/*
 * Returns the smallest CIDR which contains both of the inputs.
 */
Datum inet_merge(PG_FUNCTION_ARGS) {
    inet *a1 = PG_GETARG_INET_PP(0);  // First inet address
    inet *a2 = PG_GETARG_INET_PP(1);  // Second inet address

    // Ensure both addresses are from same family (IPv4 or IPv6)
    if (ip_family(a1) != ip_family(a2)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot merge addresses from different families")));
    }

    // Calculate number of common leading bits between addresses
    int commonbits = bitncommon(ip_addr(a1), ip_addr(a2),
                                Min(ip_bits(a1), ip_bits(a2)));

    // Return CIDR with mask length set to common bits
    PG_RETURN_INET_P(cidr_set_masklen_internal(a1, commonbits));
}
```