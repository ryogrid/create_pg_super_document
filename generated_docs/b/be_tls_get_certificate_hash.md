# be_tls_get_certificate_hash

## Location
[src/backend/libpq/be-secure-openssl.c:1555-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1555-L1617)

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
- [Hash](../H/Hash.md) size can vary depending on the algorithm used (e.g., SHA-256 = 32 bytes, SHA-512 = 64 bytes)
- Critical for SCRAM-SHA-256-PLUS authentication which requires certificate channel binding

## Simplified Source

```c
char *be_tls_get_certificate_hash(Port *port, size_t *len)
{
    X509 *server_cert;
    char *cert_hash;
    const EVP_MD *algo_type = NULL;
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_size;
    int algo_nid;

    *len = 0;

    // Get the server certificate from SSL connection
    server_cert = SSL_get_certificate(port->ssl);
    if (server_cert == NULL)
        return NULL;

    // Determine the hash algorithm based on certificate signature
    // Use newer API if available, fallback to legacy method
#if HAVE_X509_GET_SIGNATURE_INFO
    if (!X509_get_signature_info(server_cert, &algo_nid, NULL, NULL, NULL))
#else
    if (!OBJ_find_sigid_algs(X509_get_signature_nid(server_cert), &algo_nid, NULL))
#endif
        elog(ERROR, "could not determine server certificate signature algorithm");

    // RFC 5929: Use SHA-256 for weak algorithms (MD5/SHA-1), otherwise use same as signature
    switch (algo_nid)
    {
        case NID_md5:
        case NID_sha1:
            algo_type = EVP_sha256();  // Force SHA-256 for weak algorithms
            break;
        default:
            algo_type = EVP_get_digestbynid(algo_nid);  // Use signature algorithm
            if (algo_type == NULL)
                elog(ERROR, "could not find digest for NID %s", OBJ_nid2sn(algo_nid));
            break;
    }

    // Generate the certificate hash
    if (!X509_digest(server_cert, algo_type, hash, &hash_size))
        elog(ERROR, "could not generate server certificate hash");

    // Allocate and copy the hash result
    cert_hash = palloc(hash_size);
    memcpy(cert_hash, hash, hash_size);
    *len = hash_size;

    return cert_hash;
}
```