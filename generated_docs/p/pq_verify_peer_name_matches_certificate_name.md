# pq_verify_peer_name_matches_certificate_name

## Location
[src/interfaces/libpq/fe-secure-common.c:87-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-common.c#L87-L156)

## Overview
Verifies whether a name extracted from a server's SSL/TLS certificate matches the peer's hostname, supporting both exact matches and wildcard certificates.

## Definition
```c
int pq_verify_peer_name_matches_certificate_name(PGconn *conn,
                                                const char *namedata, size_t namelen,
                                                char **store_name)
```

## Detailed Description
This function performs hostname verification for SSL/TLS certificate validation in PostgreSQL's libpq client library. It extracts and validates a name from the certificate against the connection's target hostname, implementing security measures to prevent common certificate validation attacks.

The function performs several critical security checks:
- Validates that a hostname is specified in the connection
- Creates a null-terminated copy of the certificate name data
- Detects and rejects embedded null bytes to prevent CVE-2009-4034-style attacks
- Attempts both exact hostname matching and wildcard certificate matching
- Returns the extracted certificate name for caller inspection

The verification process uses case-insensitive comparison for exact matches and delegates wildcard matching to the  function for pattern-based validation.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing hostname and error handling context
- `namedata`: Raw certificate name data (may not be null-terminated)
- `namelen`: Length of the certificate name data in bytes
- `store_name`: Output parameter for the extracted certificate name (caller must free)

## Dependencies
- Functions called/Symbols referenced:
  - malloc (C standard library)
  - [wildcard_certificate_match](../w/wildcard_certificate_match.md) (PostgreSQL wildcard matching)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (libpq error handling)
  - [pg_strcasecmp](pg_strcasecmp.md) (PostgreSQL case-insensitive string comparison)
  - memcpy (C standard library)
  - strlen (C standard library)
  - free (C standard library)
- Called from (representative examples):
  - [openssl_verify_peer_name_matches_certificate_name](../o/openssl_verify_peer_name_matches_certificate_name.md)

## Notes and Other Information
- Returns 1 on successful match, 0 on no match, -1 on error
- The caller is responsible for freeing the memory allocated for `*store_name`
- Implements protection against embedded null byte attacks in certificate names
- Uses case-insensitive hostname comparison following DNS conventions
- The function handles both the common name and subject alternative names from certificates
- Memory allocation failures and malformed certificate names trigger appropriate error messages in the connection context