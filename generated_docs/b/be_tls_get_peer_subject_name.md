# be_tls_get_peer_subject_name

## Location
[src/backend/libpq/be-secure-openssl.c:1516-1524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1516-L1524)

## Overview
Retrieves the subject name from the peer's X.509 certificate and copies it to a provided buffer.

## Definition
```c
void be_tls_get_peer_subject_name(Port *port, char *ptr, size_t len)
```

## Detailed Description
This function extracts the subject name from the peer's X.509 certificate in a TLS connection and copies it to a user-provided buffer. The subject name contains identifying information about the certificate holder, typically including the Common Name (CN), Organization (O), Country (C), and other distinguished name attributes.

The function first checks if a peer certificate is available in the port structure. If a certificate exists, it extracts the subject name using OpenSSL functions, converts it to a string representation, and safely copies it to the destination buffer using `strlcpy`. If no peer certificate is available, it sets the buffer to an empty string.

This information is crucial for certificate-based authentication and auditing, as it identifies who the peer certificate was issued to.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing a client connection with potential peer certificate
- `ptr`: Destination buffer to receive the subject name string
- `len`: Size of the destination buffer to prevent buffer overflow

## Dependencies
- Functions called/Symbols referenced:
  - X509_get_subject_name (OpenSSL function to get subject from certificate)
  - [X509_NAME_to_cstring](../X/X509_NAME_to_cstring.md) (PostgreSQL wrapper to convert X509_NAME to string)
  - [strlcpy](../s/strlcpy.md) (safe string copy function)
  - [Port](../P/Port.md) (structure containing peer certificate)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (for collecting connection statistics with certificate info)

## Notes and Other Information
- Sets the buffer to empty string if no peer certificate is available
- Uses `strlcpy` for safe string copying with buffer length protection
- The subject name format follows X.509 distinguished name conventions
- Part of PostgreSQL's certificate validation and auditing infrastructure
- The peer certificate is typically populated during the TLS handshake process
- Subject name is used for certificate-based authentication and access control
- Located in src/backend/libpq/be-secure-openssl.c:1516-1524

## Simplified Source

```c
// Simplified version of be_tls_get_peer_subject_name
void be_tls_get_peer_subject_name(Port *port, char *ptr, size_t len) {
    // Copy subject name if peer certificate exists
    if (port->peer) {
        char *subject_name = X509_NAME_to_cstring(X509_get_subject_name(port->peer));
        strlcpy(ptr, subject_name, len);
    } else {
        // Set empty string if no peer certificate
        ptr[0] = '\0';
    }
}
```

Key simplifications made:
- Extracted subject name conversion to a separate variable for clarity
- Added explanatory comments for each major step
- Core logic: Get subject name from peer certificate or set empty string