# network_supeq

## Location
[src/backend/utils/adt/network.c:948-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L948-L962)

## Overview
Implements the supernet-or-equal (>>= operator) comparison for INET data types, checking if the first network contains or is equal to the second network.

## Definition
```c
Datum network_supeq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `network_supeq` function implements PostgreSQL's supernet-or-equal operator (>>=) for INET data types. It determines whether the first network argument contains or is equal to the second network argument. This is a containment relationship where the first network's address space includes the second network's address space.

The function first checks if both networks belong to the same IP family (IPv4 or IPv6). If they do, it performs the containment check by:
1. Verifying that the first network's prefix length is less than or equal to the second network's prefix length
2. Comparing the network portions of both addresses using bitwise comparison for the length of the first network's prefix

If the networks belong to different IP families, the function returns false as cross-family containment is not meaningful.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `a1`: First INET value (potential supernet)  
  - `a2`: Second INET value (potential subnet)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP (extract INET arguments)
  - ip_family (get IP family of network)
  - ip_bits (get prefix length of network)
  - ip_addr (get network address)
  - [bitncmp](../b/bitncmp.md) (bitwise comparison of network addresses)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - No direct references found (likely called via SQL operator >>=)

## Notes and Other Information
- This function implements the >>= operator in SQL for INET types
- Returns true if the first network contains or equals the second network
- Cross-family comparisons (IPv4 vs IPv6) always return false
- The containment logic requires the supernet to have a prefix length less than or equal to the subnet
- Used internally by PostgreSQL's network operator system for INET comparisons

## Simplified Source

```c
Datum network_supeq(PG_FUNCTION_ARGS) {
    inet *a1 = PG_GETARG_INET_PP(0);  // First network (potential supernet)
    inet *a2 = PG_GETARG_INET_PP(1);  // Second network (potential subnet)

    // Check if both networks are same IP family (IPv4 or IPv6)
    if (ip_family(a1) == ip_family(a2)) {
        // Supernet check: first prefix <= second prefix AND addresses match
        PG_RETURN_BOOL(ip_bits(a1) <= ip_bits(a2) &&
                       bitncmp(ip_addr(a1), ip_addr(a2), ip_bits(a1)) == 0);
    }

    // Different IP families cannot have containment relationship
    PG_RETURN_BOOL(false);
}
```