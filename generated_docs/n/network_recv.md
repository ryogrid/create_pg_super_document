# network_recv

## Location
[src/backend/utils/adt/network.c:192-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L192-L249)

## Overview
Converts external binary format to internal inet representation, serving as the core deserialization function for both inet and cidr data types.

## Definition
```c
static inet *network_recv(StringInfo buf, bool is_cidr)
```

## Detailed Description
The `network_recv` function is a static helper function that deserializes network address data from PostgreSQL's external binary format into the internal inet structure. It handles both inet and cidr data types by parsing the binary stream containing address family, subnet bits, CIDR flag, address length, and the actual network address bytes. The function includes comprehensive validation to ensure the received data is well-formed and represents a valid network address. For CIDR values, it additionally validates that no bits are set beyond the subnet mask boundary.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the serialized binary data to be parsed
- `is_cidr`: Boolean flag indicating whether to apply CIDR-specific validation rules

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory for the inet structure
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md): Reads single bytes from the binary message buffer
  - `ip_family`, `ip_bits`, `ip_addr`, `ip_addrsize`, `ip_maxbits`: Inet structure accessor macros
  - `PGSQL_AF_INET`, `PGSQL_AF_INET6`: Address family constants
  - `ereport`, `errcode`, `errmsg`, `errdetail`: Error reporting functions
  - [addressOK](../a/addressOK.md): Validates that CIDR addresses have no bits set beyond the mask
  - `SET_INET_VARSIZE`: Sets the variable size header for the inet structure
- Called from (representative examples):
  - [inet_recv](../i/inet_recv.md): Public function for inet type deserialization
  - [cidr_recv](../c/cidr_recv.md): Public function for cidr type deserialization

## Notes and Other Information
- The external binary format consists of: family byte, bits byte, is_cidr byte, address length byte, followed by address bytes in network byte order
- The is_cidr byte in the external format is largely historical and is ignored during input processing
- For CIDR types, the function enforces that no host bits are set beyond the network mask
- Supports both IPv4 (PGSQL_AF_INET) and IPv6 (PGSQL_AF_INET6) address families
- Uses zero-initialized memory allocation to ensure unused bits are cleared
- Located in src/backend/utils/adt/network.c:192-249

## Simplified Source

```c
static inet *
network_recv(StringInfo buf, bool is_cidr)
{
    inet *addr = (inet *) palloc0(sizeof(inet));
    char *addrptr;
    int bits, nb, i;

    // Read address family and validate
    ip_family(addr) = pq_getmsgbyte(buf);
    if (ip_family(addr) != PGSQL_AF_INET && ip_family(addr) != PGSQL_AF_INET6)
        ereport(ERROR, "invalid address family in external value");

    // Read and validate subnet bits
    bits = pq_getmsgbyte(buf);
    if (bits < 0 || bits > ip_maxbits(addr))
        ereport(ERROR, "invalid bits in external value");
    ip_bits(addr) = bits;

    pq_getmsgbyte(buf);  // Skip is_cidr flag (historical)

    // Read and validate address length
    nb = pq_getmsgbyte(buf);
    if (nb != ip_addrsize(addr))
        ereport(ERROR, "invalid length in external value");

    // Read address bytes
    addrptr = (char *) ip_addr(addr);
    for (i = 0; i < nb; i++)
        addrptr[i] = pq_getmsgbyte(buf);

    // For CIDR: validate no bits set beyond mask
    if (is_cidr && !addressOK(ip_addr(addr), bits, ip_family(addr)))
        ereport(ERROR, "invalid external cidr value");

    SET_INET_VARSIZE(addr);
    return addr;
}
```