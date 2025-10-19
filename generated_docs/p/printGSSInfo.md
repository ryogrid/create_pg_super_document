# printGSSInfo

## Location
[src/bin/psql/command.c:3999-4014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3999-L4014)

## Overview
Displays information about GSSAPI encryption status if the current database connection is using GSSAPI encryption.

## Definition

```c
static void
printGSSInfo(void)
```
## Detailed Description
The  function provides users with information about GSSAPI (Generic Security Services Application Program Interface) encryption on their database connection. GSSAPI encryption provides an alternative to SSL/TLS for securing database communications, often used in enterprise environments with Kerberos authentication.

Key behaviors include:
- **GSSAPI encryption detection**: Checks if the current connection is using GSSAPI encryption before displaying any information
- **Simple status display**: Shows a straightforward message confirming GSSAPI encryption is active
- **Silent operation**: Returns immediately if GSSAPI encryption is not in use, avoiding unnecessary output
- **Localization support**: Uses translatable strings for proper internationalization

The function is intentionally minimal compared to  because GSSAPI encryption typically provides fewer configurable parameters that users need to see, focusing instead on the binary question of whether encryption is active.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - : Checks whether the connection is using GSSAPI encryption
- Called from (representative examples):
  - : During connection establishment
  - : When displaying connection information via \conninfo command

## Notes and Other Information
- GSSAPI encryption is an alternative to SSL/TLS that integrates with enterprise authentication systems like Kerberos
- The function's simplicity reflects that GSSAPI encryption parameters are typically less complex than SSL/TLS configurations
- GSSAPI encryption is particularly common in enterprise PostgreSQL deployments that use centralized authentication
- Unlike SSL connections, GSSAPI doesn't expose detailed protocol or cipher information through libpq's standard interface
- The function serves as a security confirmation tool, letting users verify their connection is encrypted when GSSAPI is configured
- Works in conjunction with  to provide comprehensive connection security information

## Simplified Source

```c
static void printGSSInfo(void)
{
    // Only proceed if GSSAPI encryption is active
    if (!PQgssEncInUse(pset.db))
        return;

    // Display simple GSSAPI encryption confirmation
    printf("GSSAPI-encrypted connection\n");
}
```

**Simplified Logic:**
1. **Check GSSAPI status**: Return early if GSSAPI encryption is not in use
2. **Display confirmation**: Show simple message confirming GSSAPI encryption is active

This function provides a straightforward confirmation that the database connection is using GSSAPI encryption, which is an enterprise-grade alternative to SSL/TLS often used with Kerberos authentication systems.