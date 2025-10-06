# network_send

## Location
[src/backend/utils/adt/network.c:270-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L270-L291)

## Overview
Converts internal inet representation to external binary format, serving as the core serialization function for both inet and cidr data types.

## Definition
```c
static bytea *network_send(inet *addr, bool is_cidr)
```

## Detailed Description
The `network_send` function is a static helper function that serializes network address data from PostgreSQL's internal inet structure into the external binary format. It handles both inet and cidr data types by constructing a binary stream containing the address family, subnet bits, CIDR flag, address length, and the actual network address bytes in network byte order. The function creates a compact binary representation that can be transmitted over the network or stored efficiently in binary format.

## Parameters / Member Variables
- `addr`: Pointer to the inet structure containing the network address to be serialized
- `is_cidr`: Boolean flag indicating whether this is being serialized as a CIDR value (affects the is_cidr byte in output)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_begintypsend](../p/pq_begintypsend.md): Initializes the binary output buffer
  - [pq_sendbyte](../p/pq_sendbyte.md): Writes single bytes to the binary message buffer
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalizes and returns the binary output buffer as bytea
  - `ip_family`, `ip_bits`, `ip_addr`, `ip_addrsize`: Inet structure accessor macros
- Called from (representative examples):
  - [inet_send](../i/inet_send.md): Public function for inet type binary serialization
  - [cidr_send](../c/cidr_send.md): Public function for cidr type binary serialization

## Notes and Other Information
- The external binary format consists of: family byte, bits byte, is_cidr byte, address length byte, followed by address bytes in network byte order
- Handles edge cases where address size might be negative by setting it to zero
- The is_cidr parameter affects the third byte in the output format to maintain compatibility
- Binary format is more compact and efficient than text representation for network transmission
- Returns a bytea (binary data) result that can be transmitted over PostgreSQL's binary protocol
- Located in src/backend/utils/adt/network.c:270-291

## Simplified Source

```c
static bytea *
network_send(inet *addr, bool is_cidr)
{
    StringInfoData buf;
    char *addrptr;
    int nb, i;

    pq_begintypsend(&buf);

    // Write header: family, bits, is_cidr flag
    pq_sendbyte(&buf, ip_family(addr));
    pq_sendbyte(&buf, ip_bits(addr));
    pq_sendbyte(&buf, is_cidr);

    // Write address length and address bytes
    nb = ip_addrsize(addr);
    if (nb < 0) nb = 0;  // Handle edge case
    pq_sendbyte(&buf, nb);

    addrptr = (char *) ip_addr(addr);
    for (i = 0; i < nb; i++)
        pq_sendbyte(&buf, addrptr[i]);

    return pq_endtypsend(&buf);
}
```