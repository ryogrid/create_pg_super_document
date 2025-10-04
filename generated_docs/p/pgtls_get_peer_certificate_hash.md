# pgtls_get_peer_certificate_hash

## Location
[src/interfaces/libpq/fe-secure-openssl.c:362-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L362-L451)

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

## Simplified Source
```c
char *pgtls_get_peer_certificate_hash(PGconn *conn, size_t *len) {
    X509 *peer_cert;
    const EVP_MD *algo_type;
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hash_size;
    int algo_nid;
    char *cert_hash;

    *len = 0;

    // Check if peer certificate exists
    if (!conn->peer)
        return NULL;

    peer_cert = conn->peer;

    // Get certificate signature algorithm (OpenSSL version-dependent)
#if HAVE_X509_GET_SIGNATURE_INFO
    if (!X509_get_signature_info(peer_cert, &algo_nid, NULL, NULL, NULL))
#else
    if (!OBJ_find_sigid_algs(X509_get_signature_nid(peer_cert), &algo_nid, NULL))
#endif
        return NULL; // Error getting signature algorithm

    // Select hash algorithm per RFC 5929: SHA-256 for weak algorithms, otherwise same as signature
    switch (algo_nid) {
        case NID_md5:
        case NID_sha1:
            algo_type = EVP_sha256(); // Use SHA-256 for weak algorithms
            break;
        default:
            algo_type = EVP_get_digestbynid(algo_nid); // Use same as signature
            if (algo_type == NULL)
                return NULL;
            break;
    }

    // Generate certificate hash
    if (!X509_digest(peer_cert, algo_type, hash, &hash_size))
        return NULL;

    // Allocate and copy hash result
    cert_hash = malloc(hash_size);
    if (cert_hash == NULL)
        return NULL;

    memcpy(cert_hash, hash, hash_size);
    *len = hash_size;

    return cert_hash;
}
```