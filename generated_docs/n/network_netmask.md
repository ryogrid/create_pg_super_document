# network_netmask

## Location
[src/backend/utils/adt/network.c:1374-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1374-L1415)

## Overview
Generates the netmask (subnet mask) for a given network address, returning an address where network bits are set to 1 and host bits are set to 0.

## Definition

```c
Datum
network_netmask(PG_FUNCTION_ARGS)
```
## Detailed Description
This function creates the netmask (subnet mask) corresponding to a given inet or cidr network address. It takes an IP address with a prefix length and generates the appropriate netmask by setting the first N bits (where N is the prefix length) to 1 and the remaining bits to 0. The resulting address represents the subnet mask that can be used for network calculations, routing, and determining which portion of an IP address represents the network versus the host. The function sets the bits field to the maximum possible for the address family (32 for IPv4, 128 for IPv6).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the inet/cidr input network address
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Allocates zero-initialized memory for the result
  - : Gets the prefix length (netmask bits) of the address
  - : Gets pointer to the raw address bytes
  - : Gets/sets the address family
  - : Gets the maximum possible bits for the address family
  - : Sets the variable size header for the inet type
  - : Returns the inet result
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Creates a traditional subnet mask representation where network bits are 1 and host bits are 0
- The algorithm processes bytes sequentially, setting appropriate bit patterns
- For partial bytes (when prefix length is not a multiple of 8), uses left-shift operations to create the correct mask
- Sets the bits field to maximum (32 for IPv4, 128 for IPv6) rather than preserving the original prefix length
- Essential for network administration and subnet calculations
- Part of PostgreSQL's comprehensive network address manipulation functions
- Located in src/backend/utils/adt/network.c:1374-1415

## Simplified Source

```c
Datum network_netmask(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);  // Input network address
    inet *dst = (inet *) palloc0(sizeof(inet));  // Result netmask

    int bits = ip_bits(ip);           // Network prefix length
    unsigned char *b = ip_addr(dst);  // Destination address bytes
    int byte = 0;

    // Build netmask by setting network bits to 1
    while (bits > 0) {
        unsigned char mask;

        if (bits >= 8) {
            mask = 0xff;        // Set all 8 bits in this byte
            bits -= 8;
        } else {
            mask = 0xff << (8 - bits);  // Set only remaining network bits
            bits = 0;
        }

        b[byte] = mask;  // Set the mask byte
        byte++;
    }

    // Set metadata for netmask result
    ip_family(dst) = ip_family(ip);
    ip_bits(dst) = ip_maxbits(ip);  // Full address length (32 or 128)
    SET_INET_VARSIZE(dst);

    PG_RETURN_INET_P(dst);
}
```