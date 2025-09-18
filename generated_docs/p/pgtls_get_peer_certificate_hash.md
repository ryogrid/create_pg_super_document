# pgtls_get_peer_certificate_hash

## Location
src/interfaces/libpq/fe-secure-openssl.c: 362 - 451

## Overview
Generates a cryptographic hash of the peer's certificate for TLS channel binding, implementing RFC 5929 requirements for certificate hash generation.

## Definition
```c
char *pgtls_get_peer_certificate_hash(PGconn *conn, size_t *len)
```

## Detailed Description
This function computes a hash of the server's certificate that is used for TLS channel binding in SCRAM authentication. The function follows RFC 5929 specifications, which require that certificates signed with MD5 or SHA-1 algorithms be hashed using SHA-256 for security reasons, while certificates with other signature algorithms use the same hash algorithm as their signature.

The function first examines the certificate's signature algorithm using either X509_get_signature_info() (OpenSSL 1.1.1+) or the older OBJ_find_sigid_algs() method for backwards compatibility. Based on the signature algorithm, it selects the appropriate hash algorithm and computes the certificate hash.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure containing the TLS connection information
- `len`: Output parameter that receives the length of the generated hash

## Dependencies
- Functions called/Symbols referenced:
  - X509_get_signature_info (OpenSSL 1.1.1+)
  - OBJ_find_sigid_algs (fallback for older OpenSSL)
  - X509_get_signature_nid
  - EVP_sha256
  - EVP_get_digestbynid
  - X509_digest
  - malloc
  - memcpy
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Called from (representative examples):
  - [build_client_final_message](../b/build_client_final_message.md) (SCRAM authentication)

## Notes and Other Information
- Returns NULL on error and sets appropriate error message via libpq_append_conn_error
- Caller is responsible for freeing the returned hash buffer
- Uses EVP_MAX_MD_SIZE buffer size to accommodate various hash algorithms including SHA-512
- Implements security requirements from RFC 5929 section 4.1 regarding weak signature algorithms
- The conn->peer field must be set (contains the X509 certificate) before calling this function