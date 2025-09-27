# pg_getaddrinfo_all

## Location
[src/common/ip.c:53-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/ip.c#L53-L81)

## Overview
Provides a unified interface for getting address information across Unix domain sockets, IPv4, and IPv6 connections, serving as PostgreSQL's wrapper around the standard getaddrinfo() function.

## Definition
```c
int pg_getaddrinfo_all(const char *hostname, const char *servname,
                      const struct addrinfo *hintp, struct addrinfo **result)
```

## Detailed Description
This function acts as PostgreSQL's centralized address resolution interface, handling both network sockets (IPv4/IPv6) and Unix domain sockets through a single API. It determines the socket family from the hints parameter and routes Unix socket requests to a specialized handler while passing network socket requests to the standard system getaddrinfo() function.

The function ensures consistent behavior across different platforms by initializing the result pointer to NULL before processing, addressing variations in getaddrinfo() implementations that may not clear the result on failure.

## Parameters / Member Variables
- `hostname`: The hostname or IP address to resolve (NULL or empty string has special meaning for getaddrinfo)
- `servname`: The service name or port number
- `hintp`: Pointer to addrinfo structure containing hints for address resolution (particularly ai_family)
- `result`: Pointer to store the resulting linked list of addrinfo structures

## Dependencies
- Functions called/Symbols referenced:
  - [getaddrinfo_unix](../g/getaddrinfo_unix.md) (for Unix domain socket handling)
  - getaddrinfo (standard system call for network sockets)
- Called from (representative examples):
  - [ident_inet](../i/ident_inet.md) (authentication)
  - [PerformRadiusTransaction](../P/PerformRadiusTransaction.md) (RADIUS authentication)
  - [parse_hba_line](parse_hba_line.md) (HBA configuration parsing)
  - [ListenServerPort](../L/ListenServerPort.md) (server connection setup)
  - [PQconnectPoll](../P/PQconnectPoll.md) (client connection establishment)

## Notes and Other Information
- Special handling for AF_UNIX family addresses through getaddrinfo_unix()
- Ensures result pointer is always initialized to prevent undefined behavior
- Empty or NULL hostname is passed as NULL to getaddrinfo() for special binding behavior
- Located in src/common/ip.c:53-81, making it available to both frontend and backend code

## Simplified Source

```c
// Simplified version of pg_getaddrinfo_all
int pg_getaddrinfo_all(const char *hostname, const char *servname,
                      const struct addrinfo *hintp, struct addrinfo **result) {
    // Initialize result to NULL for consistent behavior across platforms
    *result = NULL;

    // Route Unix domain socket requests to specialized handler
    if (hintp->ai_family == AF_UNIX) {
        return getaddrinfo_unix(servname, hintp, result);
    }

    // Handle network sockets (IPv4/IPv6) using standard getaddrinfo
    // Convert empty hostname to NULL for special getaddrinfo behavior
    const char *resolved_hostname = (!hostname || hostname[0] == '\0') ? NULL : hostname;

    return getaddrinfo(resolved_hostname, servname, hintp, result);
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Extracted hostname resolution logic into a clearer variable
- Emphasized the dual-path routing (Unix vs network sockets)
- Maintained the essential algorithm and error handling patterns