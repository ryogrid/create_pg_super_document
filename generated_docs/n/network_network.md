# network_network

## Location
[src/backend/utils/adt/network.c:1330-1373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1330-L1373)

## Overview
Extracts the network portion of an IP address by zeroing out all host bits, effectively returning the network address of a subnet.

## Definition

```c
Datum
network_network(PG_FUNCTION_ARGS)
```
## Detailed Description
This function computes the network address from a given inet or cidr address by applying the network mask to zero out the host portion. It takes an IP address with a prefix length and returns the network address by preserving only the network bits (as specified by the prefix length) and setting all host bits to zero. The function works by applying a bitwise AND operation with appropriate masks to each byte of the address, effectively extracting only the network portion while maintaining the original prefix length and address family.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the inet/cidr input network address
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Allocates zero-initialized memory for the result
  - : Gets the prefix length (netmask bits) of the address
  - : Gets pointer to the raw address bytes
  - : Gets/sets the address family
  - : Sets the variable size header for the inet type
  - : Returns the inet result
- Called from (representative examples):
  -  (src/backend/utils/adt/network.c:1692)

## Notes and Other Information
- The algorithm processes address bytes sequentially, applying network masks
- For each byte, it determines how many network bits to preserve and creates appropriate masks
- Uses bitwise AND operations to zero out host bits beyond the network prefix
- The resulting address represents the network identifier for the subnet
- Preserves the original address family and prefix length in the result
- Essential for network operations like routing table lookups and subnet identification
- Located in src/backend/utils/adt/network.c:1330-1373

## Simplified Source

```c
Datum network_network(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);  // Input network address
    inet *dst = (inet *) palloc0(sizeof(inet));  // Result network address

    int bits = ip_bits(ip);           // Network prefix length
    unsigned char *a = ip_addr(ip);   // Source address bytes
    unsigned char *b = ip_addr(dst);  // Destination address bytes
    int byte = 0;

    // Process bytes to preserve network bits and zero host bits
    while (bits > 0) {
        unsigned char mask;

        if (bits >= 8) {
            mask = 0xff;        // Preserve all 8 bits in this byte
            bits -= 8;
        } else {
            mask = 0xff << (8 - bits);  // Preserve only remaining network bits
            bits = 0;
        }

        b[byte] = a[byte] & mask;  // Apply mask to zero host bits
        byte++;
    }

    // Copy metadata and return result
    ip_family(dst) = ip_family(ip);
    ip_bits(dst) = ip_bits(ip);
    SET_INET_VARSIZE(dst);

    PG_RETURN_INET_P(dst);
}
```