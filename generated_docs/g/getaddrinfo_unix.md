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

## Simplified Source

```c
// Simplified version of getaddrinfo_unix
static int getaddrinfo_unix(const char *path, const struct addrinfo *hintsp,
                           struct addrinfo **result) {
    struct addrinfo hints = {0};
    struct addrinfo *aip;
    struct sockaddr_un *unp;

    *result = NULL;

    // Validate path length
    if (strlen(path) >= sizeof(unp->sun_path))
        return EAI_FAIL;

    // Set up hints - default to AF_UNIX/SOCK_STREAM
    if (hintsp == NULL) {
        hints.ai_family = AF_UNIX;
        hints.ai_socktype = SOCK_STREAM;
    } else {
        memcpy(&hints, hintsp, sizeof(hints));
    }

    // Default to SOCK_STREAM if not specified
    if (hints.ai_socktype == 0)
        hints.ai_socktype = SOCK_STREAM;

    // Validate family is AF_UNIX
    if (hints.ai_family != AF_UNIX)
        return EAI_FAIL;

    // Allocate addrinfo structure
    aip = calloc(1, sizeof(struct addrinfo));
    if (aip == NULL)
        return EAI_MEMORY;

    // Allocate Unix socket address structure
    unp = calloc(1, sizeof(struct sockaddr_un));
    if (unp == NULL) {
        free(aip);
        return EAI_MEMORY;
    }

    // Initialize addrinfo structure
    aip->ai_family = AF_UNIX;
    aip->ai_socktype = hints.ai_socktype;
    aip->ai_protocol = hints.ai_protocol;
    aip->ai_addr = (struct sockaddr *) unp;
    aip->ai_addrlen = sizeof(struct sockaddr_un);
    *result = aip;

    // Set up Unix socket address
    unp->sun_family = AF_UNIX;
    strcpy(unp->sun_path, path);

    // Handle abstract namespace sockets (@ prefix)
    if (path[0] == '@') {
        unp->sun_path[0] = '\0';  // Replace @ with null byte
        aip->ai_addrlen = offsetof(struct sockaddr_un, sun_path) + strlen(path);
    }

    return 0;
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added inline comments for each major step
- Preserved all essential logic and error handling
- Maintained proper memory management
- Kept the abstract socket namespace handling intact
- Simplified the structure without losing functionality