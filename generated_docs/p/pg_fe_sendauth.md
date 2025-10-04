# pg_fe_sendauth

## Location
[src/interfaces/libpq/fe-auth.c:961-1168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L961-L1168)

## Overview
Core client-side authentication dispatcher that processes authentication requests from the PostgreSQL server and sends appropriate responses based on the authentication method requested.

## Definition

```c
int
pg_fe_sendauth(AuthRequest areq, int payloadlen, PGconn *conn)
```
## Detailed Description
 serves as the main demultiplexer for handling authentication challenges from the PostgreSQL server. When the server sends an authentication request, this function analyzes the authentication method type and dispatches to the appropriate authentication handler. It supports multiple authentication mechanisms including GSS/SSPI, SASL, MD5, and password authentication.

The function assumes that the authentication message has been completely read into the input buffer, with the caller having already processed the message type and length. It handles the remaining payload data specific to each authentication method.

Key responsibilities include:
- Validating expected authentication requests against connection state
- Routing to method-specific authentication handlers (GSS, SSPI, SASL, password)
- Managing thread safety for authentication processes
- Setting connection state flags for authentication completion
- Providing comprehensive error handling and reporting

## Parameters / Member Variables
- `areq`: Authentication request type identifier (AUTH_REQ_OK, AUTH_REQ_GSS, AUTH_REQ_SASL, etc.)
- `payloadlen`: Number of remaining bytes in the authentication message to be processed
- `*conn`: PostgreSQL connection object containing authentication state and credentials
## Dependencies
- Functions called/Symbols referenced:
  - [check_expected_areq](../c/check_expected_areq.md)
  - [pg_GSS_startup](pg_GSS_startup.md), pg_SSPI_startup (GSS/SSPI authentication)
  - [pg_GSS_continue](pg_GSS_continue.md), pg_SSPI_continue (GSS/SSPI continuation)
  - [pg_password_sendauth](pg_password_sendauth.md) (password/MD5 authentication)
  - [pg_SASL_init](pg_SASL_init.md), pg_SASL_continue (SASL authentication)
  - pglock_thread, pgunlock_thread (thread safety)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
- Called from (representative examples):
  - Connection establishment routines in libpq

## Notes and Other Information
- Thread-safe: Uses pglock_thread/pgunlock_thread for GSS/SSPI operations
- Compile-time conditional support for GSS, SSPI authentication methods
- Sets  for password/MD5 methods indicating no further authentication expected
- Returns STATUS_OK on success, STATUS_ERROR on failure
- Comprehensive error messages for unsupported or failed authentication methods
- Legacy Kerberos 4/5 and Crypt authentication explicitly unsupported

## Simplified Source

```c
int
pg_fe_sendauth(AuthRequest areq, int payloadlen, PGconn *conn)
{
    // Validate the authentication request meets security requirements
    if (!check_expected_areq(areq, conn)) {
        return STATUS_ERROR;
    }

    switch (areq) {
        case AUTH_REQ_OK:
            // Authentication successful
            break;

        case AUTH_REQ_KRB4:
        case AUTH_REQ_KRB5:
        case AUTH_REQ_CRYPT:
            // Legacy authentication methods not supported
            libpq_append_conn_error(conn, "Authentication method not supported");
            return STATUS_ERROR;

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
        case AUTH_REQ_GSS:
        case AUTH_REQ_SSPI:
            // Start GSS/SSPI authentication
            pglock_thread();
            int result = pg_GSS_startup(conn, payloadlen); // or SSPI variant
            if (result != STATUS_OK) {
                pgunlock_thread();
                return STATUS_ERROR;
            }
            pgunlock_thread();
            break;

        case AUTH_REQ_GSS_CONT:
            // Continue GSS/SSPI authentication
            pglock_thread();
            result = pg_GSS_continue(conn, payloadlen); // or SSPI variant
            if (result != STATUS_OK) {
                pgunlock_thread();
                return STATUS_ERROR;
            }
            pgunlock_thread();
            break;
#endif

        case AUTH_REQ_MD5:
        case AUTH_REQ_PASSWORD:
            // Handle password authentication
            char *password = conn->connhost[conn->whichhost].password;
            if (!password) password = conn->pgpass;

            if (!password || password[0] == '\0') {
                appendPQExpBufferStr(&conn->errorMessage, "no password supplied");
                return STATUS_ERROR;
            }

            if (pg_password_sendauth(conn, password, areq) != STATUS_OK) {
                appendPQExpBufferStr(&conn->errorMessage, "password authentication failed");
                return STATUS_ERROR;
            }

            conn->client_finished_auth = true;
            break;

        case AUTH_REQ_SASL:
            // Initialize SASL authentication
            if (pg_SASL_init(conn, payloadlen) != STATUS_OK) {
                return STATUS_ERROR;
            }
            break;

        case AUTH_REQ_SASL_CONT:
        case AUTH_REQ_SASL_FIN:
            // Continue or finalize SASL authentication
            if (!conn->sasl_state) {
                appendPQExpBufferStr(&conn->errorMessage, "invalid SASL state");
                return STATUS_ERROR;
            }

            if (pg_SASL_continue(conn, payloadlen, (areq == AUTH_REQ_SASL_FIN)) != STATUS_OK) {
                appendPQExpBufferStr(&conn->errorMessage, "SASL authentication failed");
                return STATUS_ERROR;
            }
            break;

        default:
            libpq_append_conn_error(conn, "authentication method %u not supported", areq);
            return STATUS_ERROR;
    }

    return STATUS_OK;
}
```