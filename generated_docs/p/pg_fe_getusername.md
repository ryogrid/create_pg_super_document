# pg_fe_getusername

## Location
[src/interfaces/libpq/fe-auth.c:1169-1213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L1169-L1213)

## Overview
Retrieves the username associated with a specified user ID, providing cross-platform user name resolution for PostgreSQL client authentication.

## Definition

```c
char *
pg_fe_getusername(uid_t user_id, PQExpBuffer errorMessage)
```
## Detailed Description
 is a platform-abstraction function that retrieves the textual username corresponding to a given user ID. It handles the differences between Windows and Unix-like systems for user name lookup operations.

On Unix-like systems, it uses the provided  parameter to perform a lookup via . On Windows, the  parameter is ignored and the function always retrieves the current user's name using the Windows API  function.

The function allocates memory for the returned username string using , making the caller responsible for freeing the memory. It provides comprehensive error handling and reporting through the optional  parameter.

## Parameters / Member Variables
- : User ID to look up (ignored on Windows, used on Unix-like systems)
- : Optional buffer for error message reporting; if NULL, errors are not reported

## Dependencies
- Functions called/Symbols referenced:
  - GetUserName (Windows API for current user lookup)
  - [pg_get_user_name](pg_get_user_name.md) (Unix user ID to name conversion)
  - [libpq_append_error](../l/libpq_append_error.md) (error message formatting)
  - strdup (string duplication/memory allocation)
- Called from (representative examples):
  - [pg_fe_getauthname](pg_fe_getauthname.md)
  - CONNECTION_FAILED (connection establishment)

## Notes and Other Information
- Returns malloc'd memory that must be freed by caller
- Platform-specific behavior: Windows ignores user_id parameter
- Returns NULL on failure with optional error message population
- Thread-safe on platforms where underlying system calls are thread-safe
- Buffer sizes: Windows uses UNLEN+1 (257 chars), Unix uses BUFSIZ for temporary storage
- Handles out-of-memory conditions gracefully with appropriate error reporting

## Simplified Source

```c
char *pg_fe_getusername(uid_t user_id, PQExpBuffer errorMessage) {
    char *result = NULL;
    const char *name = NULL;

#ifdef WIN32
    // Windows: get current user name (user_id parameter ignored)
    char username[257];  // UNLEN+1 where UNLEN=256
    DWORD namesize = sizeof(username);

    if (GetUserName(username, &namesize)) {
        name = username;
    } else if (errorMessage) {
        libpq_append_error(errorMessage, "user name lookup failure: error code %lu", GetLastError());
    }
#else
    // Unix: lookup user by ID
    char pwdbuf[BUFSIZ];

    if (pg_get_user_name(user_id, pwdbuf, sizeof(pwdbuf))) {
        name = pwdbuf;
    } else if (errorMessage) {
        appendPQExpBuffer(errorMessage, "%s\n", pwdbuf);
    }
#endif

    // Duplicate the name string if lookup succeeded
    if (name) {
        result = strdup(name);
        if (result == NULL && errorMessage)
            libpq_append_error(errorMessage, "out of memory");
    }

    return result;
}
```