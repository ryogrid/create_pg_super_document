# openssl_verify_peer_name_matches_certificate_name

## Location
src/interfaces/libpq/fe-secure-openssl.c: 492 - 524

## Overview
OpenSSL-specific wrapper function that converts ASN1_STRING certificate names to plain C strings for peer name verification in TLS connections.

## Definition
```c
static int openssl_verify_peer_name_matches_certificate_name(PGconn *conn, ASN1_STRING *name_entry, char **store_name)
```

## Detailed Description
This function serves as an adapter between OpenSSL's ASN.1 string representation and PostgreSQL's generic certificate name verification logic. It extracts the raw string data from an OpenSSL ASN1_STRING structure and converts it into a format suitable for the platform-independent name matching function.

The function handles the extraction of name data using the appropriate OpenSSL API (either ASN1_STRING_get0_data for newer versions or ASN1_STRING_data for older versions), then delegates the actual name matching logic to the generic pq_verify_peer_name_matches_certificate_name function. This design allows the core verification logic to remain independent of the specific TLS library implementation.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure for error reporting and connection context
- `name_entry`: OpenSSL ASN1_STRING structure containing the certificate name to be verified (from Subject Alternative Name or Common Name)
- `store_name`: Output parameter for storing the extracted name string (if requested by caller)

## Dependencies
- Functions called/Symbols referenced:
  - ASN1_STRING_get0_data (OpenSSL 1.1.0+) or ASN1_STRING_data (legacy)
  - ASN1_STRING_length
  - pq_verify_peer_name_matches_certificate_name (generic verification function)
  - libpq_append_conn_error (for error reporting)
- Called from (representative examples):
  - pgtls_verify_peer_name_matches_certificate_guts (during SAN processing)
  - pgtls_verify_peer_name_matches_certificate_guts (during Common Name processing)

## Notes and Other Information
- Returns -1 on error, or the result from pq_verify_peer_name_matches_certificate_name
- Assumes GEN_DNS names are IA5String format (equivalent to US-ASCII) as per X.509 standards
- Uses conditional compilation to support both old and new OpenSSL API versions
- Performs safe casting from unsigned char to char since certificate names are ASCII
- Validates that name_entry is not NULL before processing
- Part of the certificate hostname verification chain in PostgreSQL's TLS implementation