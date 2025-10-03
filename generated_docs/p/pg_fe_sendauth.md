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