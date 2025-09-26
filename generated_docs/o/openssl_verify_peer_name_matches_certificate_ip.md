# openssl_verify_peer_name_matches_certificate_ip

## Location
[src/interfaces/libpq/fe-secure-openssl.c:525-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L525-L553)

## Overview
OpenSSL-specific wrapper function that extracts IP addresses from ASN1_OCTET_STRING structures for peer IP address verification in TLS connections.

## Definition
```c
static int openssl_verify_peer_name_matches_certificate_ip(PGconn *conn, ASN1_OCTET_STRING *addr_entry, char **store_name)
```

## Detailed Description
This function serves as an OpenSSL-specific adapter for IP address verification in TLS certificates. It extracts binary IP address data from OpenSSL's ASN1_OCTET_STRING structure and converts it into a format suitable for PostgreSQL's generic IP address matching logic.

The function handles the GEN_IPADD type from Subject Alternative Names (SAN), which contains IP addresses in network byte order as binary data. It uses the appropriate OpenSSL API to extract the raw bytes and delegates the actual IP address comparison to the platform-independent verification function.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure for error reporting and connection context
- `addr_entry`: OpenSSL ASN1_OCTET_STRING structure containing the binary IP address from the certificate's Subject Alternative Name
- `store_name`: Output parameter for storing the IP address string representation (if requested by caller)

## Dependencies
- Functions called/Symbols referenced:
  - ASN1_STRING_get0_data (OpenSSL 1.1.0+) or ASN1_STRING_data (legacy)
  - ASN1_STRING_length
  - [pq_verify_peer_name_matches_certificate_ip](../p/pq_verify_peer_name_matches_certificate_ip.md) (generic IP verification function)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (for error reporting)
- Called from (representative examples):
  - [pgtls_verify_peer_name_matches_certificate_guts](../p/pgtls_verify_peer_name_matches_certificate_guts.md) (during SAN IP address processing)

## Notes and Other Information
- Returns -1 on error, or the result from pq_verify_peer_name_matches_certificate_ip
- Handles IP addresses stored in network byte order as per X.509 standards
- Uses conditional compilation to support both old and new OpenSSL API versions
- Validates that addr_entry is not NULL before processing
- Part of the certificate hostname/IP verification chain in PostgreSQL's TLS implementation
- Supports both IPv4 (4 bytes) and IPv6 (16 bytes) addresses as determined by the length
- The binary format allows exact matching without string parsing ambiguities