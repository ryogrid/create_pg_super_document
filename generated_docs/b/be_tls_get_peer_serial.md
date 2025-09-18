# be_tls_get_peer_serial

## Location
src/backend/libpq/be-secure-openssl.c: 1534 - 1554

## Overview
Extracts the serial number from the peer's X.509 certificate and converts it to a decimal string representation.

## Definition


## Detailed Description
This function retrieves the serial number from the peer's X.509 certificate stored in the port structure and converts it to a decimal string format. The function uses OpenSSL APIs to extract the ASN.1 INTEGER serial number, convert it to a BIGNUM, and then to a decimal string representation. If no peer certificate is available, the output buffer is set to an empty string.

The serial number is a unique identifier assigned by the Certificate Authority (CA) when issuing the certificate, making it useful for certificate identification and logging purposes.

## Parameters / Member Variables
- : Pointer to the Port structure containing the connection state, including the peer certificate
- : Output buffer to store the decimal string representation of the serial number
- : Size of the output buffer to prevent buffer overflow

## Dependencies
- Functions called/Symbols referenced:
  - X509_get_serialNumber (OpenSSL function)
  - ASN1_INTEGER_to_BN (OpenSSL function)
  - BN_bn2dec (OpenSSL function)
  - BN_free (OpenSSL function)
  - strlcpy (PostgreSQL utility function)
  - OPENSSL_free (OpenSSL memory deallocation)
- Called from (representative examples):
  - [pgstat_bestart](../p/pgstat_bestart.md) (backend status reporting)

## Notes and Other Information
- The function safely handles the case where no peer certificate is present by setting the output to an empty string
- Memory management is properly handled with BN_free() and OPENSSL_free() to prevent memory leaks
- Uses strlcpy() for safe string copying with length bounds checking
- The serial number is converted from ASN.1 INTEGER to BIGNUM to decimal string for human-readable output
- Part of the TLS/SSL certificate information extraction functionality in PostgreSQL