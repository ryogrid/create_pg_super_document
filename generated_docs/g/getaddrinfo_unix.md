# getaddrinfo_unix

## Location
[src/common/ip.c:153-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/ip.c#L153-L227)

## Overview
Provides getaddrinfo-compatible address resolution functionality specifically for Unix domain sockets, creating addrinfo structures for local socket connections.

## Definition
```c
static int getaddrinfo_unix(const char *path, const struct addrinfo *hintsp,
                           struct addrinfo **result)
```

## Detailed Description
This static function implements Unix domain socket address resolution using the same API pattern as the standard getaddrinfo() function. It creates a properly formatted addrinfo structure containing a sockaddr_un address for the specified Unix socket path. The function supports both regular Unix sockets and abstract namespace sockets (indicated by a leading '@' character).

The implementation handles path length validation, memory allocation, and proper initialization of both the addrinfo structure and the embedded sockaddr_un address. It supports the abstract socket namespace convention where paths beginning with '@' are converted to paths starting with a null byte, with the address length adjusted accordingly.

## Parameters / Member Variables
- `path`: The Unix socket path (supports both regular and abstract '@' prefixed paths)
- `hintsp`: Optional hints structure for socket type and protocol preferences (NULL allowed)
- `result`: Pointer to store the resulting addrinfo structure

## Dependencies
- Functions called/Symbols referenced:
  - calloc (memory allocation)
  - strcpy (string copying)
  - memcpy (memory copying)
  - strlen (string length calculation)
  - [sockaddr_un](../s/sockaddr_un.md) (Unix socket address structure)
- Called from:
  - [pg_getaddrinfo_all](../p/pg_getaddrinfo_all.md) (when ai_family is AF_UNIX)

## Notes and Other Information
- Static function, only accessible within src/common/ip.c
- Validates path length against sun_path size limit to prevent buffer overflow
- Defaults to SOCK_STREAM if no socket type specified in hints
- Supports abstract namespace sockets via '@' prefix convention
- Returns EAI_FAIL for invalid family or oversized paths, EAI_MEMORY for allocation failures
- Does not support AI_CANONNAME flag (documented limitation)
- Only creates single addrinfo entry regardless of hints
- Handles memory cleanup on allocation failure to prevent leaks
- Located in src/common/ip.c:153-227