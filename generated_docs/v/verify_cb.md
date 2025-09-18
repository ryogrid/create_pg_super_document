# verify_cb

## Location
src/backend/libpq/be-secure-openssl.c: 1193 - 1271

## Overview
A certificate verification callback function that examines SSL/TLS certificate validation failures and collects detailed information for logging purposes.

## Definition


## Detailed Description
This function serves as OpenSSL's certificate verification callback, allowing PostgreSQL to examine intermediate problems during certificate validation and collect detailed information for later logging. While the function currently accepts OpenSSL's default verification criteria, it provides comprehensive error reporting by extracting certificate details when verification fails. The callback extracts certificate subject, issuer, serial number, and error information to create detailed error messages that help with SSL/TLS troubleshooting. The function stores the formatted error details in cert_errdetail for subsequent logging by the calling code.

## Parameters / Member Variables
- : Boolean indicating whether the certificate verification passed (1) or failed (0)
- : X509_STORE_CTX pointer containing the certificate verification context and error information

## Dependencies
- Functions called/Symbols referenced:
  - [X509_NAME_to_cstring](../X/X509_NAME_to_cstring.md) (converts X509_NAME to C string)
  - [prepare_cert_name](../p/prepare_cert_name.md) (sanitizes certificate names for safe logging)
  - X509_STORE_CTX_get_error_depth (gets verification error depth)
  - X509_STORE_CTX_get_error (gets verification error code)
  - X509_verify_cert_error_string (gets error description)
  - X509_STORE_CTX_get_current_cert (gets current certificate)
  - Various OpenSSL functions for certificate parsing
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md) (src/backend/libpq/be-secure-openssl.c:349)
  - [initialize_SSL](../i/initialize_SSL.md) (src/interfaces/libpq/fe-secure-openssl.c:1463)

## Notes and Other Information
- Returns the original 'ok' value without modification (accepts default verification behavior)
- Collects detailed certificate information only on verification failures
- Prevents log flooding by sanitizing certificate names through prepare_cert_name()
- Extracts certificate serial numbers to help disambiguate certificates with similar subjects
- Sets global cert_errdetail variable with formatted error information
- Mirrors functionality from be_tls_get_peer_serial() for serial number extraction
- Critical for SSL/TLS debugging and security audit trails in PostgreSQL