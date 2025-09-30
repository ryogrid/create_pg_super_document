# pg_get_user_home_dir

## Location
[src/port/user.c:64-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/user.c#L64-L89)

## Overview
Retrieves the home directory path associated with a given user ID (UID) from the system's user database, providing thread-safe user home directory lookup functionality.

## Definition

```c
struct passwd pwdstr;
```
## Detailed Description
The  function performs a thread-safe lookup of a user's home directory based on their numeric user ID. It uses the POSIX  function to query the system's user database and retrieve the home directory path from the user's passwd entry. The function follows the same error handling pattern as , providing localized error messages when lookups fail.

On successful lookup, the function copies the home directory path into the provided buffer and returns . If the lookup fails, it populates the buffer with an appropriate error message and returns . An important design note is that this function does not check the  environment variable, as it is specifically designed to query the system database for a particular user ID rather than the current user's environment.

## Parameters / Member Variables
- : The numeric user ID (UID) whose home directory should be looked up in the system user database
- : Output buffer where the home directory path (on success) or error message (on failure) will be stored
- : Size of the output buffer in bytes, used to prevent buffer overflows

## Dependencies
- Functions called/Symbols referenced:
  -  (POSIX thread-safe user lookup)
  -  (safe string copying)
  -  (formatted string output)
  -  (thread-safe error message retrieval)
- Called from (representative examples):
  -  (libpq home directory resolution)
  -  (path utility functions)

## Notes and Other Information
- This function is thread-safe due to its use of  instead of the non-reentrant 
- Uses a local buffer  to store the passwd structure data required by 
- Deliberately does not check the  environment variable, as documented in the source comments
- Error messages are localized using the  macro for internationalization support
- Part of PostgreSQL's portability layer, providing consistent home directory lookup across different platforms
- Returns the  field from the passwd structure, which contains the user's home directory path
- Used by PostgreSQL components that need to resolve user home directories for configuration files or data storage

## Simplified Source

```c
bool pg_get_user_home_dir(uid_t user_id, char *buffer, size_t buflen) {
    char pwdbuf[BUFSIZ];
    struct passwd pwdstr;
    struct passwd *pw = NULL;
    int pwerr;

    // Thread-safe lookup of user information by ID
    pwerr = getpwuid_r(user_id, &pwdstr, pwdbuf, sizeof(pwdbuf), &pw);

    if (pw != NULL) {
        // Success: copy home directory path
        strlcpy(buffer, pw->pw_dir, buflen);
        return true;
    }

    // Handle lookup failure with appropriate error message
    if (pwerr != 0) {
        snprintf(buffer, buflen,
                _("could not look up local user ID %d: %s"),
                (int) user_id,
                strerror_r(pwerr, pwdbuf, sizeof(pwdbuf)));
    } else {
        snprintf(buffer, buflen,
                _("local user with ID %d does not exist"),
                (int) user_id);
    }

    return false;
}
```