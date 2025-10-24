# pg_set_block

## Location
[src/port/noblock.c:49-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/noblock.c#L49-L66)

## Overview
Sets a socket to blocking mode, ensuring I/O operations will wait for completion before returning.

## Definition

```c
bool
pg_set_block(pgsocket sock)
```
## Detailed Description
The `pg_set_block` function configures a socket to operate in blocking mode. In blocking mode, I/O operations on the socket will wait (block) until the operation can be completed, which is the default behavior for most socket operations. This function is typically used to restore normal blocking behavior after a socket has been set to non-blocking mode.

The function uses platform-specific system calls:
- On Unix-like systems: Uses `fcntl()` with `F_GETFL` and `F_SETFL` flags to clear the `O_NONBLOCK` flag
- On Windows: Uses `ioctlsocket()` with the `FIONBIO` command, setting the parameter to 0

## Parameters / Member Variables
- `sock`: A PostgreSQL socket descriptor (`pgsocket` type) that will be configured for blocking operation

## Dependencies
- Functions called/Symbols referenced:
  - `fcntl()` (Unix/Linux systems)
  - `ioctlsocket()` (Windows systems) 
  - `pgsocket` (PostgreSQL socket type)
- Called from (representative examples):
  - Used in socket management and connection handling routines

## Notes and Other Information
- Returns `true` on success, `false` on failure
- The function handles cross-platform differences between Unix/Linux and Windows socket APIs
- On Unix systems, it clears the `O_NONBLOCK` flag using bitwise AND with the complement (`~O_NONBLOCK`)
- On Windows, `ioctlsocket()` is called with `ioctlsocket_ret = 0` to disable non-blocking mode
- This function is the complement to `pg_set_noblock()` and is used to restore standard blocking socket behavior
- Primarily used when transitioning from asynchronous to synchronous socket operations
- Should be used with proper error handling to ensure socket state changes are successful

## Simplified Source

```c
bool
pg_set_block(pgsocket sock)
{
#if !defined(WIN32)
    // Unix/Linux: Use fcntl to clear O_NONBLOCK flag
    int flags = fcntl(sock, F_GETFL);
    if (flags < 0)
        return false;
    if (fcntl(sock, F_SETFL, (flags & ~O_NONBLOCK)) == -1)
        return false;
    return true;
#else
    // Windows: Use ioctlsocket to disable non-blocking mode
    unsigned long ioctlsocket_ret = 0;
    return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}
```