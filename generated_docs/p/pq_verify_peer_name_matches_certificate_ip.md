# pq_verify_peer_name_matches_certificate_ip

## Location
src/interfaces/libpq/fe-secure-common.c: 157 - 251

## Overview
Validates whether an IP address extracted from a server's SSL/TLS certificate matches the peer's hostname when the hostname is also an IP address.

## Definition
```c
int pq_verify_peer_name_matches_certificate_ip(PGconn *conn,
                                              const unsigned char *ipdata,
                                              size_t iplen,
                                              char **store_name)
```

## Detailed Description
This function performs IP address verification for SSL/TLS certificate validation in PostgreSQL's libpq client library. It compares binary IP address data from a certificate's Subject Alternative Name extension against the connection's target IP address hostname.

The function supports both IPv4 and IPv6 address verification:
- IPv4 addresses (4 bytes) are processed using `inet_aton()` for flexible address notation support
- IPv6 addresses (16 bytes) are processed using `inet_pton()` when available
- Both certificate and hostname addresses are converted to network byte order for comparison
- Invalid IP lengths trigger errors rather than being silently ignored

The verification process includes generating a human-readable string representation of the certificate's IP address for caller inspection and debugging purposes.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing hostname and error handling context
- `ipdata`: Binary IP address data from the certificate (network byte order)
- `iplen`: Length of the IP address data (4 for IPv4, 16 for IPv6)
- `store_name`: Output parameter for the string representation of certificate IP (caller must free)

## Dependencies
- Functions called/Symbols referenced:
  - PG_STRERROR_R_BUFLEN (PostgreSQL error buffer size constant)
  - [inet_aton](../i/inet_aton.md) (C standard library IPv4 conversion)
  - [pg_inet_net_ntop](pg_inet_net_ntop.md) (PostgreSQL network address to string conversion)
  - strerror_r (C standard library error string conversion)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (libpq error handling)
  - memcmp (C standard library)
  - strdup (C standard library)
- Called from (representative examples):
  - [openssl_verify_peer_name_matches_certificate_ip](../o/openssl_verify_peer_name_matches_certificate_ip.md)

## Notes and Other Information
- Returns 1 on successful match, 0 on no match, -1 on error
- The caller is responsible for freeing the memory allocated for `*store_name`
- Uses `inet_aton()` for IPv4 to support alternative address notations beyond standard dotted decimal
- IPv6 support is conditional on `HAVE_INET_PTON` compilation flag
- Rejects certificates with IP addresses of invalid lengths as a security measure
- Network byte order comparison ensures correct cross-platform behavior
- The function treats hostname-to-IP conversion failures as mismatches rather than errors
- Generates detailed error messages for debugging certificate validation issues