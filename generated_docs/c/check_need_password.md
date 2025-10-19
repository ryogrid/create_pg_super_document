# check_need_password

## Location
[src/bin/initdb/initdb.c:2575-2588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2575-L2588)

## Overview
Validates whether a password must be specified for the superuser when password-based authentication methods are configured for both local and host connections in PostgreSQL initialization.

## Definition
static void check_need_password(const char *authmethodlocal, const char *authmethodhost)

## Detailed Description
This function performs a critical validation during PostgreSQL database initialization (initdb). It checks if both local and host authentication methods require password-based authentication (md5, password, or scram-sha-256), and ensures that a superuser password has been provided either through interactive prompt or password file. If password authentication is configured but no password mechanism is available, the function terminates the initialization process with a fatal error.

The function implements a safety check to prevent the creation of a PostgreSQL cluster with password authentication enabled but no way for the superuser to authenticate, which would result in an unusable database system.

## Parameters / Member Variables
- `authmethodlocal`: Authentication method configured for local connections (typically from pg_hba.conf template)
- `authmethodhost`: Authentication method configured for host (network) connections (typically from pg_hba.conf template)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (C standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error handling function)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3440)

## Notes and Other Information
- This function uses global variables `pwprompt` and `pwfilename` to determine if a password mechanism has been configured
- The function only checks for password-required authentication methods: "md5", "password", and "scram-sha-256"
- Other authentication methods like "trust", "peer", "ident", etc., do not require password validation
- This validation occurs during the initdb process to ensure the resulting database cluster is properly configured and accessible

## Simplified Source

```c
static void check_need_password(const char *authmethodlocal, const char *authmethodhost) {
    // Check if both local and host connections require password authentication
    bool local_needs_password = (strcmp(authmethodlocal, "md5") == 0 ||
                                  strcmp(authmethodlocal, "password") == 0 ||
                                  strcmp(authmethodlocal, "scram-sha-256") == 0);

    bool host_needs_password = (strcmp(authmethodhost, "md5") == 0 ||
                                strcmp(authmethodhost, "password") == 0 ||
                                strcmp(authmethodhost, "scram-sha-256") == 0);

    // If both need passwords but no password source provided, fail
    if (local_needs_password && host_needs_password && !(pwprompt || pwfilename)) {
        pg_fatal("must specify a password for the superuser to enable password authentication");
    }
}
```