# pg_inet_cidr_ntop

## Location
src/backend/utils/adt/inet_cidr_ntop.c: 56 - 84

## Overview
Converts a network number from binary format to presentation format with CIDR-style notation, supporting both IPv4 and IPv6 addresses.

## Definition


## Detailed Description
This function is a generic wrapper that converts network addresses from their binary representation to a human-readable CIDR format string. It automatically determines which IP version-specific conversion function to call based on the address family parameter. The function always generates CIDR-style output (e.g., "192.168.1.0/24" or "2001:db8::/32") regardless of the input format.

The function acts as a dispatcher, delegating the actual conversion work to specialized functions for IPv4 and IPv6 addresses. It provides a unified interface for network address formatting across different IP versions.

## Parameters / Member Variables
- `af`: Address family constant (PGSQL_AF_INET for IPv4, PGSQL_AF_INET6 for IPv6)
- `src`: Pointer to the source network address in binary format
- `bits`: Number of network bits for the CIDR prefix length
- `dst`: Destination buffer to store the formatted string
- `size`: Size of the destination buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [inet_cidr_ntop_ipv4](../i/inet_cidr_ntop_ipv4.md) (for IPv4 address conversion)
  - [inet_cidr_ntop_ipv6](../i/inet_cidr_ntop_ipv6.md) (for IPv6 address conversion)
  - PGSQL_AF_INET (IPv4 address family constant)
  - PGSQL_AF_INET6 (IPv6 address family constant)
  - EAFNOSUPPORT (error code for unsupported address family)
- Called from (representative examples):
  - [cidr_abbrev](../c/cidr_abbrev.md) (network address abbreviation function)

## Notes and Other Information
- Returns a pointer to the destination buffer on success, or NULL on error
- Sets errno to EAFNOSUPPORT if an unsupported address family is provided
- The function assumes network byte order for input addresses
- Originally authored by Paul Vixie (ISC) in July 1996
- This is part of PostgreSQL's network data type support infrastructure