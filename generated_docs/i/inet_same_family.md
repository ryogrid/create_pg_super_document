# inet_same_family

## Location
[src/backend/utils/adt/network.c:1464-1475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1464-L1475)

## Overview
Determines whether two inet addresses belong to the same address family (IPv4 or IPv6).

## Definition

```c
Datum
inet_same_family(PG_FUNCTION_ARGS)
```
## Detailed Description
This function compares two inet addresses to determine if they belong to the same address family. It is used internally to validate that network operations can be performed between two addresses, ensuring they are compatible (both IPv4 or both IPv6). The function extracts the IP family information from each address and performs a simple equality comparison.

## Parameters / Member Variables
- First argument: inet address (accessed via )
- Second argument: inet address (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  -  - macro to extract inet arguments
  -  - extracts the address family from inet structure
  -  - macro to return boolean result
- Called from (representative examples):
  - Not directly referenced by other functions (likely used through SQL function calls)

## Notes and Other Information
- This function is typically exposed as a SQL function for network address comparison
- Returns true if both addresses are from the same family (IPv4 or IPv6), false otherwise
- Essential for network operations that require address family compatibility
- Located in src/backend/utils/adt/network.c:1464-1475

## Simplified Source

```c
/*
 * Returns true if the addresses are from the same family, or false. Used to
 * check that we can create a network which contains both of the networks.
 */
Datum inet_same_family(PG_FUNCTION_ARGS) {
    inet *a1 = PG_GETARG_INET_PP(0);  // First inet address
    inet *a2 = PG_GETARG_INET_PP(1);  // Second inet address

    // Compare address families (IPv4 or IPv6)
    PG_RETURN_BOOL(ip_family(a1) == ip_family(a2));
}
```