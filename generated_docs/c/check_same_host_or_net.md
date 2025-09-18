# check_same_host_or_net

## Location
src/backend/libpq/hba.c: 1204 - 1237

## Overview
Determines whether a client's IP address matches the server's network interfaces using 'samehost' or 'samenet' matching methods.

## Definition


## Detailed Description
The  function implements PostgreSQL's 'samehost' and 'samenet' matching capabilities for host-based authentication. It serves as the high-level coordinator that uses the system's network interface enumeration to determine if a client connection should be allowed based on network proximity.

The function works by:
1. Setting up a  structure with the client's address and matching method
2. Calling  to iterate through all network interfaces on the server
3. Using  as the callback function for each interface
4. Returning the result after all interfaces have been checked

The function supports two matching modes:
- **samehost**: Checks if the client is connecting from the same host (exact IP match)
- **samenet**: Checks if the client is on the same network/subnet as any of the server's interfaces

This approach provides a robust way to implement network-based access control without requiring administrators to manually specify all possible network ranges in pg_hba.conf.

## Parameters / Member Variables
- : Pointer to SockAddr structure containing the client's remote IP address information
- : IPCompareMethod enum value specifying whether to use samehost or samenet matching logic

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type for passing data between functions during interface enumeration
  - : System utility function that enumerates all network interfaces
  - : Callback function that performs the actual matching for each interface
  - : PostgreSQL's error reporting function for logging interface enumeration failures
  - : PostgreSQL's socket address wrapper structure
  - : Enumeration defining comparison methods (samehost/samenet)
- Called from:
  - : Main HBA authentication checking function at src/backend/libpq/hba.c:2542

## Notes and Other Information
- The function handles network interface enumeration errors gracefully by logging them and returning false
- It uses the callback pattern with  to avoid memory management complexities of interface enumeration
- The function initializes errno to 0 before calling  to ensure proper error detection
- Network interface enumeration is platform-specific, but this abstraction provides a consistent interface
- The function is static and only used within the HBA authentication module
- Both IPv4 and IPv6 interfaces are supported through the underlying implementation
- Early termination is implemented in the callback - once any interface matches, enumeration stops and the function returns true