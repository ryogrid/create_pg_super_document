# check_ip

## Location
src/backend/libpq/hba.c: 1163 - 1176

## Overview
Determines whether a client's IP address matches a given network address and netmask combination for host-based authentication.

## Definition


## Detailed Description
The  function is a fundamental component of PostgreSQL's HBA (Host-Based Authentication) system that performs IP address matching against network specifications. It validates whether a client's IP address falls within a specified network range by comparing the client's address family and then using subnet mask calculations.

The function first ensures that the client's address family (IPv4 or IPv6) matches the specified network's address family, then delegates the actual subnet range checking to the  utility function. This design provides a clean abstraction for network-based access control rules in pg_hba.conf.

## Parameters / Member Variables
- : Pointer to SockAddr structure containing the client's remote IP address information
- : Pointer to sockaddr structure representing the base network address to match against
- : Pointer to sockaddr structure representing the subnet mask for the network range

## Dependencies
- Functions called/Symbols referenced:
  - : Performs the actual subnet range checking with address family-specific logic
  - : PostgreSQL's socket address wrapper structure
- Called from:
  - : IP address matching callback function at src/backend/libpq/hba.c:1191 and 1196
  - : Main HBA authentication checking function at src/backend/libpq/hba.c:2532

## Notes and Other Information
- The function handles both IPv4 and IPv6 address families through the underlying  implementation
- Address family matching is performed first as an optimization to avoid unnecessary subnet calculations
- This function is static and only used within the HBA authentication module
- It serves as a wrapper around the more complex  function, providing a simpler interface for HBA-specific use cases
- The function returns immediately with false if address families don't match, making it efficient for mixed IPv4/IPv6 environments