# network_hostmask

## Location
[src/backend/utils/adt/network.c:1416-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L1416-L1463)

## Overview
Generates the hostmask for a given network address, returning an address where host bits are set to 1 and network bits are set to 0 (inverse of netmask).

## Definition

```c
Datum
network_hostmask(PG_FUNCTION_ARGS)
```
## Detailed Description
This function creates the hostmask corresponding to a given inet or cidr network address. It generates the inverse of a netmask by setting the host bits (bits beyond the network prefix length) to 1 and leaving network bits as 0. The function calculates the number of host bits by subtracting the prefix length from the maximum possible bits for the address family, then processes the address from the least significant byte backwards to set the appropriate host bits. This is useful for determining the host portion of an address and for certain network calculations that require the inverse of the traditional subnet mask.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the inet/cidr input network address
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts inet argument from function arguments
  - : Allocates zero-initialized memory for the result
  - : Gets the size of the IP address in bytes
  - : Gets the prefix length (netmask bits) of the address
  - : Gets the maximum possible bits for the address family
  - : Gets pointer to the raw address bytes
  - : Gets/sets the address family
  - : Sets the variable size header for the inet type
  - : Returns the inet result
- Called from (representative examples):
  - No direct callers found (SQL-level function)

## Notes and Other Information
- Creates the inverse of a netmask, highlighting the host portion of addresses
- Processes bytes from the end of the address backwards (least significant to most significant)
- Calculates host bits as: maximum_bits - network_prefix_length
- For partial bytes, uses right-shift operations to create the appropriate mask
- Sets the bits field to maximum (32 for IPv4, 128 for IPv6) in the result
- Useful for network analysis and understanding address space allocation within subnets
- Complements the functionality provided by network_netmask function
- Located in src/backend/utils/adt/network.c:1416-1463

## Simplified Source

```c
Datum network_hostmask(PG_FUNCTION_ARGS) {
    inet *ip = PG_GETARG_INET_PP(0);  // Input network address
    inet *dst = (inet *) palloc0(sizeof(inet));  // Result hostmask

    int maxbytes = ip_addrsize(ip);   // Address size in bytes
    int bits = ip_maxbits(ip) - ip_bits(ip);  // Number of host bits
    unsigned char *b = ip_addr(dst);  // Destination address bytes
    int byte = maxbytes - 1;          // Start from least significant byte

    // Build hostmask by setting host bits to 1 (working backwards)
    while (bits > 0) {
        unsigned char mask;

        if (bits >= 8) {
            mask = 0xff;        // Set all 8 bits in this byte
            bits -= 8;
        } else {
            mask = 0xff >> (8 - bits);  // Set only remaining host bits
            bits = 0;
        }

        b[byte] = mask;  // Set the mask byte
        byte--;
    }

    // Set metadata for hostmask result
    ip_family(dst) = ip_family(ip);
    ip_bits(dst) = ip_maxbits(ip);  // Full address length (32 or 128)
    SET_INET_VARSIZE(dst);

    PG_RETURN_INET_P(dst);
}
```