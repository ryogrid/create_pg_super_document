# auth_method_description

## Location
[src/interfaces/libpq/fe-auth.c:767-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L767-L792)

## Overview
Translates PostgreSQL authentication request codes into human-readable error messages for client applications.

## Definition

```c
static const char *
auth_method_description(AuthRequest areq)
```
## Detailed Description
The  function provides user-friendly descriptions of PostgreSQL authentication methods when they are disallowed or unexpected. It serves as a utility function to generate meaningful error messages that help users understand what authentication method the server requested.

The function uses internationalization support through  to provide localized error messages. It handles all major PostgreSQL authentication methods including password-based, GSSAPI, SSPI, and SASL authentication types.

## Parameters / Member Variables
- `areq`: AuthRequest enum value representing the authentication method requested by the server
## Dependencies
- Functions called/Symbols referenced:
  - [libpq_gettext](../l/libpq_gettext.md) (for internationalization support)
- Constants used:
  - AUTH_REQ_PASSWORD
  - AUTH_REQ_MD5
  - AUTH_REQ_GSS
  - AUTH_REQ_GSS_CONT
  - AUTH_REQ_SSPI
  - AUTH_REQ_SASL
  - AUTH_REQ_SASL_CONT
  - AUTH_REQ_SASL_FIN
- Called from:
  - [check_expected_areq](../c/check_expected_areq.md)

## Notes and Other Information
- Returns localized strings through libpq_gettext for internationalization support
- Groups related authentication methods (GSS/GSS_CONT, SASL variants) under common descriptions
- Provides a fallback message for unknown authentication types
- Used primarily in error reporting scenarios when authentication methods are rejected or unexpected
- The returned strings are static and should not be freed by the caller
- Distinguishes between cleartext passwords (AUTH_REQ_PASSWORD) and hashed passwords (AUTH_REQ_MD5) in user messages

## Simplified Source

```c
static const char *auth_method_description(AuthRequest areq) {
    switch (areq) {
        case AUTH_REQ_PASSWORD:
            return libpq_gettext("server requested a cleartext password");
        case AUTH_REQ_MD5:
            return libpq_gettext("server requested a hashed password");
        case AUTH_REQ_GSS:
        case AUTH_REQ_GSS_CONT:
            return libpq_gettext("server requested GSSAPI authentication");
        case AUTH_REQ_SSPI:
            return libpq_gettext("server requested SSPI authentication");
        case AUTH_REQ_SASL:
        case AUTH_REQ_SASL_CONT:
        case AUTH_REQ_SASL_FIN:
            return libpq_gettext("server requested SASL authentication");
    }

    return libpq_gettext("server requested an unknown authentication type");
}
```