# clean_ipv6_addr

## Location
[src/backend/utils/adt/network.c:2095-2104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L2095-L2104)

## Overview
A utility function that removes IPv6 zone identifier suffixes (the '%zone' part) from IPv6 address strings to ensure compatibility with PostgreSQL's inet storage format.

## Definition
void clean_ipv6_addr(int addr_family, char *addr)

## Detailed Description
The `clean_ipv6_addr` function serves as a compatibility workaround for PostgreSQL's current limitation in handling IPv6 zone identifiers in stored inet values. It modifies IPv6 address strings in-place by truncating them at the '%' character, effectively removing any zone specification suffix.

This function is primarily used to sanitize the output from system functions like `getnameinfo()` before feeding it to PostgreSQL's network input parsers. IPv6 zone identifiers (also known as scope IDs) are used to specify which network interface should be used for link-local addresses, but PostgreSQL's inet type doesn't currently support storing this information.

The function includes a safety check to only perform the truncation on IPv6 addresses (AF_INET6), leaving IPv4 addresses unchanged.

## Parameters / Member Variables
- `addr_family`: Integer specifying the address family (e.g., AF_INET, AF_INET6)
- `addr`: Character pointer to the address string to be cleaned (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library function)
- Called from (representative examples):
  - [fill_hba_line](../f/fill_hba_line.md)
  - [inet_client_addr](../i/inet_client_addr.md)
  - [inet_server_addr](../i/inet_server_addr.md)
  - [pg_stat_get_backend_client_addr](../p/pg_stat_get_backend_client_addr.md)
  - PG_STAT_GET_ACTIVITY_COLS

## Notes and Other Information
- This is explicitly marked as a temporary workaround ("XXX This should go away someday!")
- Modifies the input string in-place by null-terminating at the '%' character
- Only processes IPv6 addresses; IPv4 addresses are left unchanged
- Prevents PostgreSQL from failing when encountering zone-specified IPv6 addresses from system calls
- Used throughout PostgreSQL's networking subsystem where getnameinfo() output needs to be processed
- The alternative of silently ignoring zones in user input was deemed problematic
- Part of PostgreSQL's network address handling infrastructure