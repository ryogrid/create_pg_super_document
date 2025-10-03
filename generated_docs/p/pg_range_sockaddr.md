# pg_range_sockaddr

## Location
[src/backend/libpq/ifaddr.c:49-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L49-L65)

## Overview
Determines if a given address falls within the subnet specified by a network address and netmask.

## Definition

```c
int
pg_range_sockaddr(const struct sockaddr_storage *addr,
				  const struct sockaddr_storage *netaddr,
				  const struct sockaddr_storage *netmask)
```
## Detailed Description
This function performs subnet matching by checking if a given socket address lies within the range defined by a network address and netmask. It acts as a dispatcher function that handles both IPv4 and IPv6 address families by delegating to family-specific range checking functions. The function assumes that all three addresses belong to the same address family and that AF_UNIX addresses are not supported.

## Parameters / Member Variables
- `*addr`: The socket address to be checked for inclusion in the subnet
- `*netaddr`: The network address defining the base of the subnet
- `*netmask`: The netmask that defines the subnet range
## Dependencies
- Functions called/Symbols referenced:
  - [range_sockaddr_AF_INET](../r/range_sockaddr_AF_INET.md)
  - [range_sockaddr_AF_INET6](../r/range_sockaddr_AF_INET6.md)
- Called from (representative examples):
  - [check_ip](../c/check_ip.md)
  - IFADDR_H

## Notes and Other Information
- The caller must verify that all three addresses are in the same address family before calling this function
- AF_UNIX addresses are explicitly not supported
- Returns 0 for unsupported address families
- Returns non-zero if the address is within the specified subnet range

## Simplified Source

```c
int
pg_range_sockaddr(const struct sockaddr_storage *addr,
                  const struct sockaddr_storage *netaddr,
                  const struct sockaddr_storage *netmask)
{
    // Check if address is within subnet range based on address family
    if (addr->ss_family == AF_INET) {
        // Handle IPv4 addresses
        return range_sockaddr_AF_INET((const struct sockaddr_in *) addr,
                                    (const struct sockaddr_in *) netaddr,
                                    (const struct sockaddr_in *) netmask);
    }
    else if (addr->ss_family == AF_INET6) {
        // Handle IPv6 addresses
        return range_sockaddr_AF_INET6((const struct sockaddr_in6 *) addr,
                                     (const struct sockaddr_in6 *) netaddr,
                                     (const struct sockaddr_in6 *) netmask);
    }
    else {
        // Unsupported address family
        return 0;
    }
}
```