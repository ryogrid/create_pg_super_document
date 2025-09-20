# pg_foreach_ifaddr

## Location
[src/backend/libpq/ifaddr.c:349-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/ifaddr.c#L349-L425)

## Overview
Enumerates all network interface addresses on the system and calls a callback function for each valid address/netmask pair.

## Definition

```c
struct ifconf ifc;
```
## Detailed Description
This function provides a platform-specific interface to enumerate network interface addresses using the  system call. It dynamically allocates a buffer to retrieve all network interface configurations from the system, then iterates through each interface to extract its address and netmask information.

The function uses a growing buffer strategy, starting with 1024 bytes and expanding by 1024 bytes at a time (up to 100KB) until all interface data can be retrieved. For each valid network interface, it calls the provided callback function with the interface's address, netmask, and user-provided callback data.

The implementation handles various Unix variants that may return different amounts of data and uses heuristics to determine when the buffer is large enough to contain all interface information.

## Parameters / Member Variables
- : A function pointer of type  that will be called for each network interface. The callback receives the interface address, netmask, and user data.
- : A void pointer to user-provided data that will be passed to the callback function for each interface.

## Dependencies
- Functions called/Symbols referenced:
  - socket (system call to create UDP socket)
  - PGINVALID_SOCKET (PostgreSQL socket validity check)
  - realloc (dynamic memory allocation)
  - close (system call to close socket)
  - [run_ifaddr_callback](../r/run_ifaddr_callback.md) (internal helper function)
  - _SIZEOF_ADDR_IFREQ (macro for interface request size calculation)
- Called from (representative examples):
  - [check_same_host_or_net](../c/check_same_host_or_net.md) (in src/backend/libpq/hba.c:1213)
  - [main](../m/main.md) (in src/tools/ifaddrs/test_ifaddrs.c:68)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses AF_INET socket for ioctl operations
- Implements error handling for memory allocation failures and ioctl errors
- The buffer growing strategy handles systems that don't provide reliable buffer size requirements
- Only processes interfaces where both SIOCGIFADDR and SIOCGIFNETMASK ioctl calls succeed
- This is the ioctl-based implementation; PostgreSQL may have alternative implementations for different platforms
- The function is part of PostgreSQL's network interface abstraction layer used for host-based authentication and network configuration