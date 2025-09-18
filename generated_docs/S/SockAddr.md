# SockAddr

## Location
[src/include/libpq/pqcomm.h:34-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqcomm.h#L34-L39)

## Overview
A structure that wraps BSD socket address information with its length, providing a platform-independent way to handle network addresses in PostgreSQL's communication layer.

## Definition


## Detailed Description
SockAddr is a wrapper structure that combines a BSD socket address with its associated length information. It uses sockaddr_storage as the underlying storage type, which provides sufficient space to hold any type of socket address (IPv4, IPv6, Unix domain sockets, etc.) while maintaining proper alignment requirements.

This abstraction allows PostgreSQL to handle different types of network addresses uniformly throughout the codebase without needing to know the specific address family at compile time. The salen field stores the actual length of the address data stored in the addr field, which is essential for proper socket operations.

## Parameters / Member Variables
- : A sockaddr_storage structure that can hold any type of socket address (IPv4, IPv6, Unix domain socket, etc.)
- : The actual length in bytes of the address stored in the addr field

## Dependencies
- Functions called/Symbols referenced:
  - (Uses standard POSIX socket structures)
- Called from (representative examples):
  - [ident_inet](../i/ident_inet.md) (authentication functions)
  - [check_network_data](../c/check_network_data.md) (HBA processing)
  - [check_ip](../c/check_ip.md) (HBA IP checking)
  - [Port](../P/Port.md) structure (connection handling)
  - pg_conn structure (libpq connection management)
  - [PgBackendStatus](../P/PgBackendStatus.md) (backend status tracking)

## Notes and Other Information
- The sockaddr_storage type ensures sufficient space and proper alignment for any socket address family
- This structure is fundamental to PostgreSQL's network communication infrastructure
- Used extensively in both server-side connection handling and client-side libpq operations
- The combination of address and length follows standard BSD socket programming practices
- Enables type-safe handling of different address families without unsafe casting