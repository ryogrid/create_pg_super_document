# network_send

## Location
src/backend/utils/adt/network.c: 270 - 291

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
  - `pq_begintypsend`: Initializes the binary output buffer
  - `pq_sendbyte`: Writes single bytes to the binary message buffer
  - `pq_endtypsend`: Finalizes and returns the binary output buffer as bytea
  - `ip_family`, `ip_bits`, `ip_addr`, `ip_addrsize`: Inet structure accessor macros
- Called from (representative examples):
  - `inet_send`: Public function for inet type binary serialization
  - `cidr_send`: Public function for cidr type binary serialization

## Notes and Other Information
- The external binary format consists of: family byte, bits byte, is_cidr byte, address length byte, followed by address bytes in network byte order
- Handles edge cases where address size might be negative by setting it to zero
- The is_cidr parameter affects the third byte in the output format to maintain compatibility
- Binary format is more compact and efficient than text representation for network transmission
- Returns a bytea (binary data) result that can be transmitted over PostgreSQL's binary protocol
- Located in src/backend/utils/adt/network.c:270-291