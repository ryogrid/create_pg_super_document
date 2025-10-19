# get_id

## Location
[src/bin/initdb/initdb.c:812-830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L812-L830)

## Overview
This function determines the current user identity and ensures that initdb is not being run as the root user on Unix systems for security reasons.

## Definition

```c
static char *
get_id(void)
```
## Detailed Description
The  function is responsible for identifying the current user and performing a critical security check on Unix-like systems. It prevents PostgreSQL database initialization from being performed as the root user, which would create security vulnerabilities. On Unix systems, it uses  to check the effective user ID and exits with an error if the user is root (UID 0). After the security check passes, it retrieves the username using  and returns a dynamically allocated copy of the username string.

## Parameters / Member Variables
This function takes no parameters but returns:
- **Return value**: A dynamically allocated string containing the username (must be freed by caller)

## Dependencies
- Functions called/Symbols referenced:
  - geteuid: System call to get effective user ID (Unix only)
  - pg_log_error: For logging error messages
  - pg_log_error_hint: For providing helpful error hints
  - exit: For terminating the program on security violation
  - [get_user_name_or_exit](get_user_name_or_exit.md): For retrieving the username
  - [pg_strdup](../p/pg_strdup.md): For creating a copy of the username string
- Called from (representative examples):
  - AUTHTRUST_WARNING: Used in warning message generation
  - [main](../m/main.md): Called during initdb initialization

## Notes and Other Information
- Critical security function that prevents running initdb as root on Unix systems
- The root check is only performed on non-Windows systems (protected by )
- Exits immediately with error code 1 if root user is detected
- Provides helpful hint message suggesting to use 'su' to switch to an unprivileged user
- Returns a dynamically allocated string that must be freed by the caller
- Essential for maintaining PostgreSQL security best practices during database initialization

## Simplified Source

```c
static char *
get_id(void)
{
    const char *username;

#ifndef WIN32
    // Security check: prevent running as root on Unix systems
    if (geteuid() == 0)  /* 0 is root's uid */
    {
        pg_log_error("cannot be run as root");
        pg_log_error_hint("Please log in (using, e.g., \"su\") as the (unprivileged) user that will own the server process.");
        exit(1);
    }
#endif

    // Get the current username
    username = get_user_name_or_exit(progname);

    return pg_strdup(username);
}
```