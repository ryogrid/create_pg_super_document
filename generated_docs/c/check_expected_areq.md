# check_expected_areq

## Location
[src/interfaces/libpq/fe-auth.c:802-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L802-L960)

## Overview
Verifies that the authentication request from the server matches the client's security requirements and connection parameters.

## Definition

```c
static bool
check_expected_areq(AuthRequest areq, PGconn *conn)
```
## Detailed Description
The  function performs comprehensive validation of authentication requests from the PostgreSQL server to ensure they comply with the client's security requirements. It enforces several security policies including SSL certificate requirements, allowed authentication methods, and channel binding requirements.

The function is crucial for preventing authentication bypass attacks and ensuring that sensitive client information (like passwords) is only transmitted when appropriate security measures are in place. It handles complex scenarios including GSS encryption, SASL channel binding, and partial SCRAM exchanges.

The function implements multiple security checks: SSL certificate validation when required, authentication method allowlisting when specified by the client, channel binding enforcement for SASL authentication, and prevention of authentication downgrade attacks.

## Parameters / Member Variables
- `areq`: AuthRequest enum value representing the authentication method requested by the server
- `*conn`: Pointer to the PGconn connection structure containing security configuration and connection state
## Dependencies
- Functions called/Symbols referenced:
  - StaticAssertDecl (compile-time assertion)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - auth_method_allowed
  - [auth_method_description](../a/auth_method_description.md)
  - [libpq_gettext](../l/libpq_gettext.md)
- Constants used:
  - AUTH_REQ_OK
  - AUTH_REQ_PASSWORD
  - AUTH_REQ_MD5
  - AUTH_REQ_GSS
  - AUTH_REQ_GSS_CONT
  - AUTH_REQ_SSPI
  - AUTH_REQ_SASL
  - AUTH_REQ_SASL_CONT
  - AUTH_REQ_SASL_FIN
  - AUTH_REQ_MAX
  - CHAR_BIT
- Called from:
  - [pg_fe_sendauth](../p/pg_fe_sendauth.md)

## Notes and Other Information
- Contains compile-time assertion to ensure AUTH_REQ_MAX fits within the allowed_auth_methods bitmask
- Handles SSL certificate validation when sslcertmode=require is set
- Enforces authentication method restrictions when require_auth is specified by the client
- Special handling for GSS-encrypted connections that provide implicit authentication
- Channel binding validation ensures SASL authentication includes proper server authentication
- Prevents information leakage by rejecting inappropriate authentication methods when channel binding is required
- Returns false and sets appropriate error messages when authentication requirements are not met
- The function balances security with backward compatibility, particularly for partial SCRAM exchanges
- Critical security function that helps prevent man-in-the-middle and authentication bypass attacks

## Simplified Source

```c
static bool
check_expected_areq(AuthRequest areq, PGconn *conn)
{
    bool result = true;
    const char *reason = NULL;

    // Check SSL certificate requirements
    if (conn->sslcertmode[0] == 'r' && areq == AUTH_REQ_OK) {
        if (!conn->ssl_cert_requested || !conn->ssl_cert_sent) {
            // SSL certificate was required but not properly exchanged
            libpq_append_conn_error(conn, "SSL certificate validation failed");
            return false;
        }
    }

    // Validate authentication method against user requirements
    if (conn->require_auth) {
        switch (areq) {
            case AUTH_REQ_OK:
                // Check if authentication was actually completed
                if (!conn->auth_required || conn->client_finished_auth) {
                    break; // Valid completion
                }
#ifdef ENABLE_GSS
                if (auth_method_allowed(conn, AUTH_REQ_GSS) && conn->gssenc) {
                    break; // GSS encryption provides implicit auth
                }
#endif
                reason = "server did not complete authentication";
                result = false;
                break;

            case AUTH_REQ_PASSWORD:
            case AUTH_REQ_MD5:
            case AUTH_REQ_GSS:
            case AUTH_REQ_SASL:
                // Check if this method is allowed
                result = auth_method_allowed(conn, areq);
                break;

            default:
                result = false;
                break;
        }
    }

    // Report authentication method failures
    if (!result) {
        if (!reason) {
            reason = auth_method_description(areq);
        }
        libpq_append_conn_error(conn, "authentication requirement failed: %s", reason);
        return false;
    }

    // Enforce channel binding requirements for SASL
    if (conn->channel_binding[0] == 'r') {
        switch (areq) {
            case AUTH_REQ_SASL:
            case AUTH_REQ_SASL_CONT:
            case AUTH_REQ_SASL_FIN:
                break; // SASL methods support channel binding

            case AUTH_REQ_OK:
                if (!conn->sasl || !conn->sasl->channel_bound(conn->sasl_state)) {
                    libpq_append_conn_error(conn, "channel binding required but not completed");
                    result = false;
                }
                break;

            default:
                libpq_append_conn_error(conn, "channel binding required but not supported");
                result = false;
                break;
        }
    }

    return result;
}
```