# be_tls_get_peer_serial

## Location
[src/backend/libpq/be-secure-openssl.c:1534-1554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1534-L1554)

## Overview
Extracts the serial number from the peer's X.509 certificate and converts it to a decimal string representation.

## Definition

```c
void
be_tls_get_peer_serial(Port *port, char *ptr, size_t len)
```
## Detailed Description
This function retrieves the serial number from the peer's X.509 certificate stored in the port structure and converts it to a decimal string format. The function uses OpenSSL APIs to extract the ASN.1 INTEGER serial number, convert it to a BIGNUM, and then to a decimal string representation. If no peer certificate is available, the output buffer is set to an empty string.

The serial number is a unique identifier assigned by the Certificate Authority (CA) when issuing the certificate, making it useful for certificate identification and logging purposes.

## Parameters / Member Variables
- `*port`: Pointer to the Port structure containing the connection state, including the peer certificate
- `*ptr`: Output buffer to store the decimal string representation of the serial number
- `len`: Size of the output buffer to prevent buffer overflow
## Dependencies
- Functions called/Symbols referenced:
  - X509_get_serialNumber (OpenSSL function)
  - ASN1_INTEGER_to_BN (OpenSSL function)
  - BN_bn2dec (OpenSSL function)
  - BN_free (OpenSSL function)
  - [strlcpy](../s/strlcpy.md) (PostgreSQL utility function)
  - OPENSSL_free (OpenSSL memory deallocation)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (backend status reporting)

## Notes and Other Information
- The function safely handles the case where no peer certificate is present by setting the output to an empty string
- Memory management is properly handled with BN_free() and OPENSSL_free() to prevent memory leaks
- Uses strlcpy() for safe string copying with length bounds checking
- The serial number is converted from ASN.1 INTEGER to BIGNUM to decimal string for human-readable output
- Part of the TLS/SSL certificate information extraction functionality in PostgreSQL

## Simplified Source

```c
// Simplified version of be_tls_get_peer_serial
void be_tls_get_peer_serial(Port *port, char *ptr, size_t len) {
    // Extract serial number if peer certificate exists
    if (port->peer) {
        // Get serial number from certificate
        ASN1_INTEGER *serial = X509_get_serialNumber(port->peer);

        // Convert to BIGNUM for decimal conversion
        BIGNUM *bignum = ASN1_INTEGER_to_BN(serial, NULL);

        // Convert to decimal string
        char *decimal_string = BN_bn2dec(bignum);

        // Copy to output buffer and cleanup
        strlcpy(ptr, decimal_string, len);
        BN_free(bignum);
        OPENSSL_free(decimal_string);
    } else {
        // Set empty string if no peer certificate
        ptr[0] = '\0';
    }
}
```

Key simplifications made:
- Added descriptive variable names and inline comments
- Broke down the conversion process into clear steps
- Core logic: Extract serial from certificate, convert to decimal string, or set empty