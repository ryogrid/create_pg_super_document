# pq_verify_peer_name_matches_certificate

## Location
[src/interfaces/libpq/fe-secure-common.c:252-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-common.c#L252-L307)

## Overview
High-level function that orchestrates server certificate hostname verification for SSL/TLS connections, examining both Common Name and Subject Alternative Names.

## Definition
```c
bool pq_verify_peer_name_matches_certificate(PGconn *conn)
```

## Detailed Description
This is the primary entry point for certificate hostname verification in PostgreSQL's libpq client library. It serves as a coordinator function that determines whether hostname verification should be performed based on the SSL mode, validates prerequisites, delegates the actual verification work to SSL library-specific implementations, and formats comprehensive error messages for verification failures.

The function operates in several phases:
1. Checks if verification is required based on the SSL mode (only performs verification for "verify-full")
2. Validates that a hostname is available for comparison  
3. Delegates to `pgtls_verify_peer_name_matches_certificate_guts()` for the actual certificate parsing and verification
4. Formats detailed error messages that include certificate names to aid in debugging configuration issues
5. Handles internationalization of error messages using `libpq_ngettext()`

The error reporting is designed to be helpful for system administrators by including the first certificate name found and indicating when multiple names were examined but none matched.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing SSL mode configuration, hostname, and error handling context

## Dependencies
- Functions called/Symbols referenced:
  - pgtls_verify_peer_name_matches_certificate_guts (SSL library-specific verification implementation)
  - [libpq_ngettext](../l/libpq_ngettext.md) (PostgreSQL internationalization function)
  - appendPQExpBufferChar (libpq error message formatting)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (libpq error handling)
  - strcmp (C standard library)
  - free (C standard library)
- Called from (representative examples):
  - [open_client_SSL](../o/open_client_SSL.md)

## Notes and Other Information
- Returns `true` on successful verification or when verification is disabled, `false` on verification failure
- Only performs verification when SSL mode is set to "verify-full"
- Generates user-friendly error messages that include certificate names for debugging
- Properly handles memory management by freeing the first certificate name after use
- Supports internationalized error messages for different numbers of certificate names examined
- The actual certificate parsing and verification logic is delegated to SSL library-specific implementations
- Acts as an abstraction layer that provides consistent behavior across different SSL/TLS implementations