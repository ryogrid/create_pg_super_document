# be_tls_get_certificate_hash

## Location
src/backend/libpq/be-secure-openssl.c: 1555 - 1617

## Overview
Generates a cryptographic hash of the server's TLS certificate following RFC 5929 specifications for channel binding.

## Definition
```c
char *be_tls_get_certificate_hash(Port *port, size_t *len)
```

## Detailed Description
This function computes a hash of the server's TLS certificate for use in channel binding mechanisms, particularly for SCRAM authentication. The function follows RFC 5929 guidelines which specify that certificates signed with MD5 or SHA-1 should be hashed using SHA-256 for security reasons, while certificates using stronger signature algorithms should use the same hash algorithm as their signature.

The function retrieves the server certificate from the SSL connection, determines the appropriate hash algorithm based on the certificate's signature algorithm, computes the hash, and returns the result as a dynamically allocated buffer.

## Parameters / Member Variables
- `port`: Pointer to the Port structure containing the SSL connection information
- `len`: Output parameter that receives the length of the generated hash

## Dependencies
- Functions called/Symbols referenced:
  - SSL_get_certificate (OpenSSL function)
  - X509_get_signature_info (OpenSSL 1.1.1+ function)
  - OBJ_find_sigid_algs (OpenSSL legacy function)
  - X509_get_signature_nid (OpenSSL function)
  - EVP_sha256 (OpenSSL function)
  - EVP_get_digestbynid (OpenSSL function)
  - X509_digest (OpenSSL function)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - memcpy (standard C function)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [read_client_final_message](../r/read_client_final_message.md) (SCRAM authentication)

## Notes and Other Information
- Implements RFC 5929 channel binding specifications for TLS certificate hashing
- Uses conditional compilation to support both newer OpenSSL 1.1.1+ and older versions
- Enforces SHA-256 for certificates signed with weak algorithms (MD5, SHA-1) for security
- Returns NULL if no server certificate is available
- Memory for the hash is allocated using palloc() and should be freed by the caller
- Hash size can vary depending on the algorithm used (e.g., SHA-256 = 32 bytes, SHA-512 = 64 bytes)
- Critical for SCRAM-SHA-256-PLUS authentication which requires certificate channel binding