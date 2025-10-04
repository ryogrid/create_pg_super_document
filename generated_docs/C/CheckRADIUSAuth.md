# CheckRADIUSAuth

## Location
[src/backend/libpq/auth.c:2847-2941](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2847-L2941)

## Overview
Main entry point for RADIUS-based authentication that coordinates the entire RADIUS authentication process including server iteration and credential validation.

## Definition

```c
struct alignment is correct */
	Assert(offsetof(radius_packet, vector) == 4);
```
## Detailed Description
This static function orchestrates the complete RADIUS authentication workflow for a client connection. It validates configuration parameters, requests a password from the client, and iterates through configured RADIUS servers attempting authentication until one succeeds or all fail.

The function supports multiple RADIUS server configurations with corresponding secrets, ports, and identifiers. It implements a failover mechanism where if one server fails, it tries the next server in the list. The function handles parameter validation, password length restrictions, and manages the iteration logic for multiple server configurations.

Key responsibilities include:
- Validating RADIUS configuration parameters
- Requesting password from the client using AUTH_REQ_PASSWORD
- Enforcing password length limits (RADIUS_MAX_PASSWORD_LENGTH)  
- Iterating through configured RADIUS servers with their corresponding secrets, ports, and identifiers
- Calling PerformRadiusTransaction for each server attempt
- Setting authentication identity on successful authentication
- Managing memory cleanup for password data

## Parameters / Member Variables
- : Pointer to the Port structure containing client connection information and HBA (Host-Based Authentication) configuration including RADIUS server details

## Dependencies
- Functions called/Symbols referenced:
  - [sendAuthRequest](../s/sendAuthRequest.md) (to request password from client)
  - [recv_password_packet](../r/recv_password_packet.md) (to receive password response)
  - [PerformRadiusTransaction](../P/PerformRadiusTransaction.md) (to perform actual RADIUS authentication)
  - [set_authn_id](../s/set_authn_id.md) (to set authentication identity on success)
  - [list_head](../l/list_head.md), lnext (for list traversal)
  - ereport, errmsg (for logging)
  - [pfree](../p/pfree.md) (for memory cleanup)
- Constants referenced:
  - AUTH_REQ_PASSWORD
  - STATUS_OK, STATUS_ERROR, STATUS_EOF
  - RADIUS_MAX_PASSWORD_LENGTH
- Types referenced:
  - [Port](../P/Port.md), radius_packet
- Called from:
  - Main authentication logic at src/backend/libpq/auth.c:631

## Notes and Other Information
- This is a static function, only visible within the auth.c compilation unit
- Implements struct alignment verification for radius_packet at compile time
- Supports flexible RADIUS server configuration where secrets, ports, and identifiers can be:
  - Length 0: use defaults
  - Length 1: reuse same value for all servers  
  - Same length as servers: use corresponding values
- Password is cleared from memory after authentication attempt regardless of success/failure
- Returns STATUS_OK on successful authentication, STATUS_ERROR on failure
- Part of PostgreSQL's external authentication infrastructure for RADIUS servers

## Simplified Source

```c
static int
CheckRADIUSAuth(Port *port)
{
    char       *passwd;
    ListCell   *server, *secrets, *radiusports, *identifiers;

    // Validate RADIUS configuration
    if (port->hba->radiusservers == NIL)
    {
        ereport(LOG, (errmsg("RADIUS server not specified")));
        return STATUS_ERROR;
    }

    if (port->hba->radiussecrets == NIL)
    {
        ereport(LOG, (errmsg("RADIUS secret not specified")));
        return STATUS_ERROR;
    }

    // Request password from client
    sendAuthRequest(port, AUTH_REQ_PASSWORD, NULL, 0);
    passwd = recv_password_packet(port);
    if (passwd == NULL)
        return STATUS_EOF;

    // Validate password length
    if (strlen(passwd) > RADIUS_MAX_PASSWORD_LENGTH)
    {
        ereport(LOG, (errmsg("RADIUS authentication does not support passwords longer than %d characters",
                             RADIUS_MAX_PASSWORD_LENGTH)));
        pfree(passwd);
        return STATUS_ERROR;
    }

    // Initialize list iterators for server parameters
    secrets = list_head(port->hba->radiussecrets);
    radiusports = list_head(port->hba->radiusports);
    identifiers = list_head(port->hba->radiusidentifiers);

    // Try each RADIUS server in order
    foreach(server, port->hba->radiusservers)
    {
        int ret = PerformRadiusTransaction(lfirst(server),
                                          lfirst(secrets),
                                          radiusports ? lfirst(radiusports) : NULL,
                                          identifiers ? lfirst(identifiers) : NULL,
                                          port->user_name,
                                          passwd);

        if (ret == STATUS_OK)
        {
            // Authentication successful
            set_authn_id(port, port->user_name);
            pfree(passwd);
            return STATUS_OK;
        }
        else if (ret == STATUS_EOF)
        {
            // Hard failure - don't try more servers
            pfree(passwd);
            return STATUS_ERROR;
        }

        // Advance to next parameter values if configured with multiple values
        if (list_length(port->hba->radiussecrets) > 1)
            secrets = lnext(port->hba->radiussecrets, secrets);
        if (list_length(port->hba->radiusports) > 1)
            radiusports = lnext(port->hba->radiusports, radiusports);
        if (list_length(port->hba->radiusidentifiers) > 1)
            identifiers = lnext(port->hba->radiusidentifiers, identifiers);
    }

    // All servers failed
    pfree(passwd);
    return STATUS_ERROR;
}
```