# pg_set_noblock

## Location
[src/port/noblock.c:25-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/noblock.c#L25-L48)

## Overview
Sets a socket to non-blocking mode, allowing I/O operations to return immediately without waiting for completion.

## Definition

```c
bool
pg_set_noblock(pgsocket sock)
```
## Detailed Description
The  function configures a socket to operate in non-blocking mode. In non-blocking mode, I/O operations on the socket will return immediately rather than waiting for the operation to complete. This is essential for implementing asynchronous I/O and preventing the process from blocking on socket operations.

The function uses platform-specific system calls:
- On Unix-like systems: Uses  with  and  flags to modify the  flag
- On Windows: Uses  with the  command

## Parameters / Member Variables
- `sock`: A PostgreSQL socket descriptor ( type) that will be configured for non-blocking operation
## Dependencies
- Functions called/Symbols referenced:
  -  (Unix/Linux systems)
  -  (Windows systems)
  -  (PostgreSQL socket type)
- Called from (representative examples):
  -  (src/backend/libpq/pqcomm.c:294)
  -  (src/backend/postmaster/postmaster.c:3653)
  - Connection handling in libpq (src/interfaces/libpq/fe-connect.c:3055)

## Notes and Other Information
- Returns  on success,  on failure
- The function handles cross-platform differences between Unix/Linux and Windows socket APIs
- On Unix systems, failure can occur if  cannot get or set the socket flags
- On Windows,  returns 0 on success (unlike  which returns -1 on failure)
- This is a critical function for PostgreSQL's asynchronous I/O operations and connection management
- Should be used in conjunction with proper error handling for socket operations

## Simplified Source

```c
// Simplified version of pg_set_noblock
bool pg_set_noblock(pgsocket sock) {
    // On Unix/Linux systems: Use fcntl to set O_NONBLOCK flag
#if !defined(WIN32)
    int flags = fcntl(sock, F_GETFL);
    if (flags < 0) {
        return false;  // Failed to get current flags
    }

    // Set non-blocking flag and return success/failure
    return (fcntl(sock, F_SETFL, flags | O_NONBLOCK) != -1);

    // On Windows: Use ioctlsocket with FIONBIO
#else
    unsigned long nonblock_mode = 1;
    // ioctlsocket returns 0 on success (opposite of fcntl)
    return (ioctlsocket(sock, FIONBIO, &nonblock_mode) == 0);
#endif
}
```

Key simplifications made:
- Combined the flag setting operation into a single return statement for Unix
- Used more descriptive variable name (`nonblock_mode` instead of `ioctlsocket_ret`)
- Added clear comments explaining the platform-specific behavior
- Simplified the control flow while preserving all error checking
- Maintained the essential cross-platform logic with clearer structure