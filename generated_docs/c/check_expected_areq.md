# check_expected_areq

## Location
src/interfaces/libpq/fe-auth.c: 802 - 960

## Overview
Verifies that the authentication request from the server matches the client's security requirements and connection parameters.

## Definition


## Detailed Description
The  function performs comprehensive validation of authentication requests from the PostgreSQL server to ensure they comply with the client's security requirements. It enforces several security policies including SSL certificate requirements, allowed authentication methods, and channel binding requirements.

The function is crucial for preventing authentication bypass attacks and ensuring that sensitive client information (like passwords) is only transmitted when appropriate security measures are in place. It handles complex scenarios including GSS encryption, SASL channel binding, and partial SCRAM exchanges.

The function implements multiple security checks: SSL certificate validation when required, authentication method allowlisting when specified by the client, channel binding enforcement for SASL authentication, and prevention of authentication downgrade attacks.

## Parameters / Member Variables
- : AuthRequest enum value representing the authentication method requested by the server
- : Pointer to the PGconn connection structure containing security configuration and connection state

## Dependencies
- Functions called/Symbols referenced:
  - StaticAssertDecl (compile-time assertion)
  - libpq_append_conn_error
  - auth_method_allowed
  - auth_method_description
  - libpq_gettext
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
  - pg_fe_sendauth

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