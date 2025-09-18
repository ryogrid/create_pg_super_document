# getpeereid

## Location
[src/port/getpeereid.c:33-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/getpeereid.c#L33-L78)

## Overview
A BSD-style compatibility function that retrieves the credentials (user ID and group ID) of the peer process connected to a Unix domain socket.

## Definition
```c
int getpeereid(int sock, uid_t *uid, gid_t *gid);
```

## Detailed Description
The `getpeereid` function provides a cross-platform implementation to obtain the effective user ID and group ID of the process on the other end of a Unix domain socket connection. This function implements BSD-style `getpeereid()` for platforms that lack native support.

The implementation uses different platform-specific approaches:
- **Linux**: Uses `getsockopt(SO_PEERCRED)` with `struct ucred`
- **FreeBSD variants**: Uses `getsockopt(LOCAL_PEERCRED)` with `struct xucred`
- **Solaris**: Uses native `getpeerucred()` function
- **Unsupported platforms**: Returns `ENOSYS` error

This function is essential for peer authentication in PostgreSQL, allowing the server to verify the identity of connecting clients over Unix domain sockets.

## Parameters
- `sock`: The file descriptor of the Unix domain socket
- `uid`: Pointer to store the retrieved user ID of the peer process
- `gid`: Pointer to store the retrieved group ID of the peer process

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (Linux/FreeBSD variants)
  - getpeerucred (Solaris)
  - ucred_geteuid (Solaris)
  - ucred_getegid (Solaris)
  - ucred_free (Solaris)
  - uid_t, gid_t, socklen_t (type dependencies)

- Called from (representative examples):
  - [auth_peer](../a/auth_peer.md) (in src/backend/libpq/auth.c:1872)

## Notes and Other Information
- This function is only compiled when `HAVE_GETPEEREID` is not defined, providing a fallback implementation for platforms lacking native support
- Returns 0 on success, -1 on failure
- On unsupported platforms, sets errno to `ENOSYS` and returns -1
- Critical for PostgreSQL's peer authentication mechanism over Unix domain sockets
- The implementation carefully handles platform-specific credential structures and validation
- Part of PostgreSQL's portability layer located in src/port/