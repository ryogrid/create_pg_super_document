# pg_get_user_name

## Location
[src/port/user.c:28-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/user.c#L28-L63)

## Overview
Retrieves the username associated with a given user ID (UID) from the system's user database, providing thread-safe user lookup functionality.

## Definition

```c
struct passwd pwdstr;
```
## Detailed Description
The  function performs a thread-safe lookup of a user's name based on their numeric user ID. It uses the POSIX  function to query the system's user database (typically  or equivalent) and retrieve the corresponding username. The function is designed to provide robust error handling, returning localized error messages when the lookup fails.

On successful lookup, the function copies the username into the provided buffer and returns . If the lookup fails, it populates the buffer with an appropriate error message and returns . This dual-purpose buffer usage allows callers to handle both success and error cases uniformly.

## Parameters / Member Variables
- : The numeric user ID (UID) to look up in the system user database
- : Output buffer where the username (on success) or error message (on failure) will be stored
- : Size of the output buffer in bytes, used to prevent buffer overflows

## Dependencies
- Functions called/Symbols referenced:
  -  (POSIX thread-safe user lookup)
  -  (safe string copying)
  -  (formatted string output)
  -  (thread-safe error message retrieval)
- Called from (representative examples):
  -  (libpq frontend authentication)

## Notes and Other Information
- This function is thread-safe due to its use of  instead of the non-reentrant 
- Uses a local buffer  to store the passwd structure data required by 
- Error messages are localized using the  macro for internationalization support
- Part of PostgreSQL's portability layer, providing consistent user lookup across different platforms
- The function handles both lookup failures (user doesn't exist) and system errors (permission issues, etc.) with distinct error messages