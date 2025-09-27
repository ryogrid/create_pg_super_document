# getnameinfo_unix

## Location
[src/common/ip.c:228-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/ip.c#L228-L262)

## Overview
Provides reverse name resolution functionality specifically for Unix domain sockets, converting Unix socket addresses back to human-readable path and service name representations.

## Definition
```c
static int getnameinfo_unix(const struct sockaddr_un *sa, int salen,
                           char *node, int nodelen,
                           char *service, int servicelen,
                           int flags)
```

## Detailed Description
This static function implements reverse name resolution for Unix domain sockets, serving as the Unix socket counterpart to the standard getnameinfo() function. It converts a sockaddr_un structure back into string representations suitable for logging and display purposes. The function always returns "[local]" as the hostname (node) for Unix sockets and extracts the socket path as the service name.

The implementation handles both regular Unix sockets and abstract namespace sockets. For abstract sockets (those with a null byte as the first character), it converts the representation back to the '@' prefix convention for display purposes.

## Parameters / Member Variables
- `sa`: Pointer to the sockaddr_un structure to convert
- `salen`: Size of the socket address structure (unused but kept for API consistency)
- `node`: Buffer to store the hostname (filled with "[local]")
- `nodelen`: Size of the node buffer
- `service`: Buffer to store the service name (socket path)
- `servicelen`: Size of the service buffer
- `flags`: Control flags (unused but kept for API consistency)

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (formatted string output)
  - [sockaddr_un](../s/sockaddr_un.md) (Unix socket address structure)
- Called from:
  - [pg_getnameinfo_all](../p/pg_getnameinfo_all.md) (when address family is AF_UNIX)

## Notes and Other Information
- Static function, only accessible within src/common/ip.c
- Always returns "[local]" as the node name for Unix domain sockets
- Handles abstract namespace socket display by converting null-prefixed paths back to '@' prefix
- Validates input parameters and socket family before processing
- Returns EAI_FAIL for invalid arguments, EAI_MEMORY for buffer overflow
- Requires either node or service buffer to be non-NULL
- Buffer overflow protection through snprintf return value checking
- Does not use salen or flags parameters but maintains API compatibility
- Located in src/common/ip.c:228-262

## Simplified Source

```c
// Simplified version of getnameinfo_unix
static int getnameinfo_unix(const struct sockaddr_un *sa, int salen,
                           char *node, int nodelen,
                           char *service, int servicelen,
                           int flags) {
    // Validate input parameters
    if (sa == NULL || sa->sun_family != AF_UNIX ||
        (node == NULL && service == NULL)) {
        return EAI_FAIL;
    }

    // Fill hostname buffer with "[local]" for Unix sockets
    if (node) {
        if (snprintf(node, nodelen, "%s", "[local]") >= nodelen) {
            return EAI_MEMORY;
        }
    }

    // Fill service buffer with socket path
    if (service) {
        // Handle abstract sockets (starting with null byte)
        if (sa->sun_path[0] == '\0' && sa->sun_path[1] != '\0') {
            // Abstract socket: add '@' prefix
            if (snprintf(service, servicelen, "@%s", sa->sun_path + 1) >= servicelen) {
                return EAI_MEMORY;
            }
        } else {
            // Regular socket: use path as-is
            if (snprintf(service, servicelen, "%s", sa->sun_path) >= servicelen) {
                return EAI_MEMORY;
            }
        }
    }

    return 0; // Success
}
```

Key simplifications made:
- Combined buffer overflow checks into single condition per snprintf call
- Added descriptive comments for each major logic block
- Simplified error handling while preserving essential checks
- Clarified the distinction between abstract and regular Unix sockets
- Focused on the main execution path while maintaining correctness