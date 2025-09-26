# sockaddr_un

## Location
[src/include/port/win32/sys/un.h:11-17](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/win32/sys/un.h#L11-L17)

## Overview
The `sockaddr_un` structure represents a Unix domain socket address in PostgreSQL on Windows platforms. It provides a Windows-specific definition for Unix domain socket addressing when the system's `<afunix.h>` header is not available.

## Definition
```c
struct sockaddr_un
{
    unsigned short sun_family;
    char           sun_path[108];
};
```

## Detailed Description
This structure is defined specifically for Windows compatibility in PostgreSQL's portability layer. Windows defines this structure in `<afunix.h>`, but not all Windows toolchains include this header yet, so PostgreSQL provides its own definition to ensure consistent Unix domain socket support across different Windows development environments.

The structure follows the standard Unix domain socket address format, containing a family identifier and a filesystem path for the socket. This enables PostgreSQL to use Unix domain sockets on Windows systems that support them, providing an alternative to TCP/IP connections for local inter-process communication.

## Parameters / Member Variables
- `sun_family`: Address family identifier, typically set to `AF_UNIX` to indicate this is a Unix domain socket address
- `sun_path[108]`: Character array containing the filesystem path to the Unix domain socket, with a maximum length of 107 characters plus null terminator

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure definition with no function calls)
- Called from (representative examples):
  - `[pg_getnameinfo_all](../p/pg_getnameinfo_all.md)` (src/common/ip.c:122)
  - `[getaddrinfo_unix](../g/getaddrinfo_unix.md)` (src/common/ip.c:158, 186, 202, 218)
  - `[getnameinfo_unix](../g/getnameinfo_unix.md)` (src/common/ip.c:228)
  - `UNIXSOCK_PATH_BUFLEN` macro (src/include/libpq/pqcomm.h:60)

## Notes and Other Information
- This definition is Windows-specific and located in the Windows portability headers (`src/include/port/win32/sys/un.h`)
- The 108-byte path length matches the standard Unix `sockaddr_un` specification
- Used primarily in PostgreSQL's IP address handling functions for Unix domain socket operations
- The `UNIXSOCK_PATH_BUFLEN` macro uses this structure to determine the maximum path length for Unix sockets
- This structure enables PostgreSQL to provide consistent Unix domain socket functionality across different Windows toolchain versions