# verify_cb

## Location
[src/backend/libpq/be-secure-openssl.c:1193-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1193-L1271)

## Overview
A certificate verification callback function that examines SSL/TLS certificate validation failures and collects detailed information for logging purposes.

## Definition

```c
static int
verify_cb(int ok, X509_STORE_CTX *ctx)
```
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

## Simplified Source

```c
// Simplified version of verify_cb
static int verify_cb(int ok, X509_STORE_CTX *ctx) {
    // If verification succeeded, nothing to do
    if (ok) {
        return ok;
    }

    // Extract verification failure details
    int depth = X509_STORE_CTX_get_error_depth(ctx);
    int errcode = X509_STORE_CTX_get_error(ctx);
    const char *errstring = X509_verify_cert_error_string(errcode);

    // Build error message with basic failure info
    StringInfoData error_msg;
    initStringInfo(&error_msg);
    appendStringInfo(&error_msg,
        "Client certificate verification failed at depth %d: %s.",
        depth, errstring);

    // Get current certificate for additional details
    X509 *cert = X509_STORE_CTX_get_current_cert(ctx);
    if (cert) {
        // Extract and sanitize certificate subject/issuer names
        char *subject = X509_NAME_to_cstring(X509_get_subject_name(cert));
        char *sanitized_subject = prepare_cert_name(subject);
        pfree(subject);

        char *issuer = X509_NAME_to_cstring(X509_get_issuer_name(cert));
        char *sanitized_issuer = prepare_cert_name(issuer);
        pfree(issuer);

        // Extract certificate serial number
        ASN1_INTEGER *serial_asn1 = X509_get_serialNumber(cert);
        BIGNUM *serial_bn = ASN1_INTEGER_to_BN(serial_asn1, NULL);
        char *serial_string = BN_bn2dec(serial_bn);

        // Append detailed certificate information
        appendStringInfo(&error_msg,
            "\nFailed certificate data: subject \"%s\", serial %s, issuer \"%s\".",
            sanitized_subject,
            serial_string ? serial_string : "unknown",
            sanitized_issuer);

        // Cleanup OpenSSL structures
        BN_free(serial_bn);
        OPENSSL_free(serial_string);
        pfree(sanitized_issuer);
        pfree(sanitized_subject);
    }

    // Store error details for later logging
    cert_errdetail = error_msg.data;

    // Return original verification result (no override)
    return ok;
}
```

Key simplifications made:
- Removed detailed comments about certificate flooding prevention
- Consolidated variable declarations closer to their usage
- Simplified the certificate information extraction flow
- Removed explicit translation markers for clarity
- Focused on the main execution path while preserving all essential logic
- Maintained proper memory management and cleanup operations