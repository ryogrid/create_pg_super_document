# network_recv

## Location
src/backend/utils/adt/network.c: 192 - 249

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
  - `[palloc0](../p/palloc0.md)`: Allocates zero-initialized memory for the inet structure
  - `[pq_getmsgbyte](../p/pq_getmsgbyte.md)`: Reads single bytes from the binary message buffer
  - `ip_family`, `ip_bits`, `ip_addr`, `ip_addrsize`, `ip_maxbits`: Inet structure accessor macros
  - `PGSQL_AF_INET`, `PGSQL_AF_INET6`: Address family constants
  - `ereport`, `errcode`, `errmsg`, `errdetail`: Error reporting functions
  - `[addressOK](../a/addressOK.md)`: Validates that CIDR addresses have no bits set beyond the mask
  - `SET_INET_VARSIZE`: Sets the variable size header for the inet structure
- Called from (representative examples):
  - `[inet_recv](../i/inet_recv.md)`: Public function for inet type deserialization
  - `[cidr_recv](../c/cidr_recv.md)`: Public function for cidr type deserialization

## Notes and Other Information
- The external binary format consists of: family byte, bits byte, is_cidr byte, address length byte, followed by address bytes in network byte order
- The is_cidr byte in the external format is largely historical and is ignored during input processing
- For CIDR types, the function enforces that no host bits are set beyond the network mask
- Supports both IPv4 (PGSQL_AF_INET) and IPv6 (PGSQL_AF_INET6) address families
- Uses zero-initialized memory allocation to ensure unused bits are cleared
- Located in src/backend/utils/adt/network.c:192-249