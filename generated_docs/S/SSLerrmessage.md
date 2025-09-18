# SSLerrmessage

## Location
src/common/hmac_openssl.c: 103 - 121

## Overview
Obtains a human-readable error message string for a given OpenSSL error code, providing consistent error reporting across PostgreSQL's SSL implementation.

## Definition
```c
static const char *SSLerrmessage(unsigned long ecode)
```

## Detailed Description
SSLerrmessage is a utility function that converts OpenSSL error codes into human-readable error messages. It handles various edge cases in OpenSSL error reporting, including null returns from ERR_reason_error_string and system errno values in OpenSSL 3.0+. The function ensures that a meaningful error message is always returned, never returning NULL. For OpenSSL 3.0 and later, it includes special handling for system errno values that are no longer mapped by ERR_reason_error_string. When no standard error message is available, it formats the raw error code as a fallback.

## Parameters / Member Variables
- `ecode`: The OpenSSL error code obtained from ERR_get_error() or similar functions

## Dependencies
- Functions called/Symbols referenced:
  - strerror (for system errno mapping in OpenSSL 3.0+)
  - ERR_reason_error_string (OpenSSL function)
  - ERR_SYSTEM_ERROR (macro, OpenSSL 3.0+)
  - ERR_GET_REASON (macro, OpenSSL 3.0+)
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md)
  - [be_tls_open_server](../b/be_tls_open_server.md)
  - [be_tls_read](../b/be_tls_read.md)
  - [be_tls_write](../b/be_tls_write.md)
  - [pg_hmac_init](../p/pg_hmac_init.md)
  - [initialize_SSL](../i/initialize_SSL.md)
  - [pgtls_read](../p/pgtls_read.md)

## Notes and Other Information
- Static function defined in src/backend/libpq/be-secure-openssl.c
- Handles OpenSSL version compatibility, especially for OpenSSL 3.0+ changes
- Always returns a non-NULL string, ensuring safe usage in error reporting
- Uses a static buffer for fallback error code formatting
- Includes internationalization support with _() macro for error messages
- Critical for consistent SSL error reporting throughout PostgreSQL's SSL infrastructure