# Setup_AF_UNIX

## Location
[src/backend/libpq/pqcomm.c:719-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L719-L792)

## Overview
Configures Unix domain socket permissions including group ownership and file permissions before the server starts listening for connections.

## Definition

```c
struct group *gr;
```
## Detailed Description
This function sets up the appropriate file system permissions for Unix domain sockets used by PostgreSQL. It handles both regular Unix sockets and abstract sockets (those beginning with '@'). The function must be called before listen() to ensure that the socket has the correct permissions when connections start being accepted.

The function performs two main operations:
1. Sets the group ownership of the socket file if  is configured
2. Sets the file permissions according to 

For abstract sockets (indicated by a path starting with '@'), no file system operations are performed since these sockets don't exist in the file system.

## Parameters / Member Variables
- : The file system path to the Unix domain socket file, or abstract socket name starting with '@'

## Dependencies
- Functions called/Symbols referenced:
  - STATUS_OK (return value for success)
  - STATUS_ERROR (return value for failure) 
  - Unix_socket_group (global variable)
  - Unix_socket_permissions (global variable)
  - strtoul (C library function for string to number conversion)
  - getgrnam (system call to get group information by name)
  - chown (system call to change file ownership)
  - chmod (system call to change file permissions)
  - ereport/errmsg (PostgreSQL logging functions)

- Called from (representative examples):
  - [ListenServerPort](../L/ListenServerPort.md) (during server startup to configure socket permissions)

## Notes and Other Information
- On Windows platforms, the  configuration is not supported and generates a warning
- Abstract sockets (paths starting with '@') bypass all file system permission operations
- The function converts group names to group IDs using getgrnam() if the unix_socket_group is not numeric
- All permission changes must occur before listen() is called to prevent a security window
- Errors in setting permissions or group ownership cause the function to return STATUS_ERROR and log appropriate error messages

## Simplified Source

```c
// Simplified version of Setup_AF_UNIX
static int Setup_AF_UNIX(const char *sock_path) {
    // Skip file system operations for abstract sockets
    if (sock_path[0] == '@') {
        return STATUS_OK;
    }

    // Set socket group ownership if configured
    if (Unix_socket_group[0] != '\0') {
#ifdef WIN32
        // Windows doesn't support unix socket groups
        elog(WARNING, "unix_socket_group not supported on Windows");
#else
        gid_t group_id;

        // Convert group name/ID to numeric group ID
        if (is_numeric(Unix_socket_group)) {
            group_id = convert_to_gid(Unix_socket_group);
        } else {
            group_id = lookup_group_by_name(Unix_socket_group);
            if (group_id == INVALID_GID) {
                ereport(LOG, "group does not exist");
                return STATUS_ERROR;
            }
        }

        // Change socket file group ownership
        if (chown(sock_path, -1, group_id) == -1) {
            ereport(LOG, "could not set group ownership");
            return STATUS_ERROR;
        }
#endif
    }

    // Set socket file permissions
    if (chmod(sock_path, Unix_socket_permissions) == -1) {
        ereport(LOG, "could not set permissions");
        return STATUS_ERROR;
    }

    return STATUS_OK;
}
```

Key simplifications made:
- Abstracted detailed string-to-number conversion logic into helper concept `convert_to_gid()`
- Simplified group lookup with conceptual `lookup_group_by_name()` function
- Consolidated error handling patterns into simpler ereport calls
- Removed detailed error message formatting for clarity
- Focused on the main execution flow: check abstract socket, set group, set permissions
- Maintained the essential logic while removing implementation details