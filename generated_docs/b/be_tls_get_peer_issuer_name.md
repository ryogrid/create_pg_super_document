# be_tls_get_peer_issuer_name

## Location
[src/backend/libpq/be-secure-openssl.c:1525-1533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1525-L1533)

## Overview
Retrieves the issuer name from the peer's X.509 certificate and copies it to a provided buffer.

## Definition
```c
void be_tls_get_peer_issuer_name(Port *port, char *ptr, size_t len)
```

## Detailed Description
This function extracts the issuer name from the peer's X.509 certificate in a TLS connection and copies it to a user-provided buffer. The issuer name identifies the Certificate Authority (CA) or entity that signed and issued the certificate, typically including the CA's Common Name (CN), Organization (O), Country (C), and other distinguished name attributes.

The function first checks if a peer certificate is available in the port structure. If a certificate exists, it extracts the issuer name using OpenSSL functions, converts it to a string representation, and safely copies it to the destination buffer using `strlcpy`. If no peer certificate is available, it sets the buffer to an empty string.

This information is essential for certificate chain validation, trust verification, and security auditing, as it identifies which Certificate Authority issued the peer's certificate.

## Parameters / Member Variables
- `port`: Pointer to a Port structure representing a client connection with potential peer certificate
- `ptr`: Destination buffer to receive the issuer name string
- `len`: Size of the destination buffer to prevent buffer overflow

## Dependencies
- Functions called/Symbols referenced:
  - X509_get_issuer_name (OpenSSL function to get issuer from certificate)
  - [X509_NAME_to_cstring](../X/X509_NAME_to_cstring.md) (PostgreSQL wrapper to convert X509_NAME to string)
  - [strlcpy](../s/strlcpy.md) (safe string copy function)
  - [Port](../P/Port.md) (structure containing peer certificate)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (for collecting connection statistics with certificate info)

## Notes and Other Information
- Sets the buffer to empty string if no peer certificate is available
- Uses `strlcpy` for safe string copying with buffer length protection
- The issuer name format follows X.509 distinguished name conventions
- Part of PostgreSQL's certificate validation and trust verification infrastructure
- The peer certificate is typically populated during the TLS handshake process
- Issuer information is crucial for validating certificate chains and trust relationships
- Used in conjunction with certificate validation and access control mechanisms
- Located in src/backend/libpq/be-secure-openssl.c:1525-1533

## Simplified Source

```c
// Simplified version of be_tls_get_peer_issuer_name
void be_tls_get_peer_issuer_name(Port *port, char *ptr, size_t len) {
    // Copy issuer name if peer certificate exists
    if (port->peer) {
        char *issuer_name = X509_NAME_to_cstring(X509_get_issuer_name(port->peer));
        strlcpy(ptr, issuer_name, len);
    } else {
        // Set empty string if no peer certificate
        ptr[0] = '\0';
    }
}
```

Key simplifications made:
- Extracted issuer name conversion to a separate variable for clarity
- Added explanatory comments for each major step
- Core logic: Get issuer name from peer certificate or set empty string