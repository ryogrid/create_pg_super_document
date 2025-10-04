# auth_peer

## Location
[src/backend/libpq/auth.c:1863-1929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1863-L1929)

## Overview
The auth_peer function implements peer authentication in PostgreSQL, verifying client connections by querying the operating system for the credentials of the connecting process and validating the user against the configured usermap.

## Definition
```c
static int auth_peer(hbaPort *port)
```

## Detailed Description
This function implements the peer authentication method which allows PostgreSQL to authenticate connections based on the operating system user credentials of the connecting process. The function uses the `getpeereid()` system call to obtain the user ID (uid) and group ID (gid) of the peer process, then looks up the corresponding username using `getpwuid()`. The authenticated identity is then validated against the configured usermap to determine if the connection should be allowed.

The peer authentication method is particularly useful for local connections where the client and server are running on the same machine, as it provides a secure way to authenticate without requiring passwords by leveraging the operating system's user management.

## Parameters / Member Variables
- `port`: Pointer to hbaPort structure containing connection information including the socket file descriptor, HBA (host-based authentication) configuration, and user name from the connection request

## Dependencies
- Functions called/Symbols referenced:
  - [getpeereid](../g/getpeereid.md) (system call to get peer credentials)
  - getpwuid (system call to get user information by uid)  
  - [set_authn_id](../s/set_authn_id.md) (sets the authenticated identity for the connection)
  - [check_usermap](../c/check_usermap.md) (validates the authenticated user against the configured usermap)
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md) (error code helper for socket operations)
  - strerror (standard C library error message function)
- Called from (representative examples):
  - IDENT_PORT (referenced in auth.c:79)
  - HOSTNAME_LOOKUP_DETAIL (referenced in auth.c:591)

## Notes and Other Information
- This function is only available on platforms that support the `getpeereid()` system call (primarily Unix-like systems)
- On Windows, the function will always fail with ENOSYS (function not supported)
- The function performs error handling for cases where `getpeereid()` fails or when the user ID cannot be resolved to a username
- Authentication is considered successful once the user identity is obtained from the OS; the usermap check determines authorization
- The function sets the authenticated identity before performing the usermap check to ensure proper logging
- Returns STATUS_OK if authentication and authorization succeed, STATUS_ERROR otherwise

## Simplified Source

```c
// Simplified version of auth_peer
static int auth_peer(hbaPort *port) {
    uid_t uid;
    gid_t gid;

    // Step 1: Get credentials of the connecting process
    if (getpeereid(port->sock, &uid, &gid) != 0) {
        if (errno == ENOSYS) {
            ereport(LOG, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("peer authentication is not supported on this platform")));
        } else {
            ereport(LOG, (errcode_for_socket_access(),
                         errmsg("could not get peer credentials: %m")));
        }
        return STATUS_ERROR;
    }

#ifndef WIN32
    // Step 2: Look up username from user ID
    errno = 0;
    struct passwd *pw = getpwuid(uid);
    if (!pw) {
        int save_errno = errno;
        ereport(LOG, (errmsg("could not look up local user ID %ld: %s",
                            (long) uid,
                            save_errno ? strerror(save_errno) : "user does not exist")));
        return STATUS_ERROR;
    }

    // Step 3: Set authenticated identity and check usermap
    set_authn_id(port, pw->pw_name);
    return check_usermap(port->hba->usermap, port->user_name,
                        MyClientConnectionInfo.authn_id, false);
#else
    // Windows doesn't support peer authentication
    Assert(false);
    return STATUS_ERROR;
#endif
}
```