# check_network_callback

## Location
[src/backend/libpq/hba.c:1177-1203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L1177-L1203)

## Overview
A callback function used with  to determine if a client's IP address matches any of the server's network interfaces.

## Definition

```c
static void
check_network_callback(struct sockaddr *addr, struct sockaddr *netmask,
					   void *cb_data)
```
## Detailed Description
The  function serves as a callback for the  utility, which iterates through all network interfaces on the server machine. For each interface, this callback determines whether the client's IP address matches the interface's network range.

The function supports two matching modes:
1. **Same Host Matching ()**: Creates an all-ones netmask for exact IP address matching, effectively checking if the client is connecting from the same host as the server
2. **Network Interface Matching**: Uses the actual netmask of the network interface to check if the client is on the same subnet as the interface

The callback uses early termination - once a match is found, it sets the result flag and subsequent interface checks will be skipped. This optimization is important when a server has many network interfaces.

## Parameters / Member Variables
- : Pointer to sockaddr structure representing a network interface's address
- : Pointer to sockaddr structure representing the network interface's subnet mask
- : Void pointer to check_network_data structure containing client address and result information

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type for passing data to the callback
  - : Creates an all-ones netmask for exact address matching
  - : Performs the actual IP address and network matching logic
  - : Enumeration constant indicating same-host matching mode
- Called from:
  - : Host/network matching function at src/backend/libpq/hba.c:1213

## Notes and Other Information
- This is a callback function designed to work with PostgreSQL's  network interface iteration utility
- The function implements early termination by checking if a result has already been found before proceeding
- The same-host matching mode creates a host-specific netmask (/32 for IPv4, /128 for IPv6) for exact IP matching
- Network interface matching uses the actual subnet mask of each interface, allowing for subnet-based access control
- The callback modifies the  field in the  structure passed through 
- This function is static and only used within the HBA authentication module
- It supports both IPv4 and IPv6 through the underlying  and  functions