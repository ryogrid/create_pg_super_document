# pg_getnameinfo_all

## Location
src/common/ip.c: 114 - 152

## Overview
Provides a unified interface for reverse name resolution across Unix domain sockets, IPv4, and IPv6 connections, serving as PostgreSQL's enhanced wrapper around the standard getnameinfo() function.

## Definition
```c
int pg_getnameinfo_all(const struct sockaddr_storage *addr, int salen,
                      char *node, int nodelen,
                      char *service, int servicelen,
                      int flags)
```

## Detailed Description
This function acts as PostgreSQL's centralized reverse name resolution interface, converting socket addresses back to hostnames and service names. It differs from the standard getnameinfo() API in two important ways: it uses sockaddr_storage for better type safety and size handling, and it guarantees that output buffers are always filled with meaningful content, even on failure.

The function routes Unix domain socket requests to a specialized handler while delegating network socket requests to the system's getnameinfo(). On failure, it fills the output buffers with "???" to ensure consistent behavior and prevent undefined content in the output strings.

## Parameters / Member Variables
- `addr`: Pointer to sockaddr_storage structure containing the socket address to resolve
- `salen`: Size of the socket address structure
- `node`: Buffer to store the resolved hostname (guaranteed to be filled)
- `nodelen`: Size of the node buffer
- `service`: Buffer to store the resolved service name (guaranteed to be filled)
- `servicelen`: Size of the service buffer
- `flags`: Flags controlling the resolution behavior (same as getnameinfo flags)

## Dependencies
- Functions called/Symbols referenced:
  - getnameinfo_unix (for Unix domain socket handling)
  - getnameinfo (standard system function)
  - strlcpy (safe string copying)
  - sockaddr_un (Unix socket address structure)
- Called from (representative examples):
  - ClientAuthentication (authentication logging)
  - ident_inet (ident authentication)
  - check_hostname (HBA hostname verification)
  - BackendInitialize (connection logging)
  - inet_client_addr (network function implementations)
  - pg_stat_get_backend_client_addr (statistics functions)

## Notes and Other Information
- Enhanced API using sockaddr_storage instead of sockaddr for better type safety
- Guarantees non-empty output strings by filling with "???" on failure
- Handles both forward and reverse DNS lookups through flags parameter
- Special handling for AF_UNIX addresses through getnameinfo_unix()
- Located in src/common/ip.c:114-152, available to both frontend and backend code
- Critical for logging, authentication, and network statistics functionality