# X509_NAME_to_cstring

## Location
src/backend/libpq/be-secure-openssl.c: 1618 - 1690

## Overview
Converts an X.509 certificate subject or issuer name from ASN.1 format to a human-readable C string representation.

## Definition
```c
static char *X509_NAME_to_cstring(X509_NAME *name)
```

## Detailed Description
This static function converts an X.509 certificate name (such as subject or issuer) from its internal ASN.1 representation to a readable string format following RFC 2253 conventions. The function iterates through all name entries, extracts field names and values, and formats them as a slash-separated string (e.g., "/CN=example.com/O=Organization/C=US").

The function uses OpenSSL's BIO (Basic Input/Output) mechanism as a temporary buffer to build the string, applies proper UTF-8 encoding conversion, and ensures the result is compatible with PostgreSQL's string handling and server encoding.

## Parameters / Member Variables
- `name`: Pointer to an X509_NAME structure containing the certificate name to convert

## Dependencies
- Functions called/Symbols referenced:
  - BIO_new (OpenSSL function for creating BIO)
  - BIO_s_mem (OpenSSL memory BIO type)
  - X509_NAME_entry_count (OpenSSL function)
  - X509_NAME_get_entry (OpenSSL function)
  - OBJ_obj2nid (OpenSSL object identifier function)
  - X509_NAME_ENTRY_get_object (OpenSSL function)
  - X509_NAME_ENTRY_get_data (OpenSSL function)
  - OBJ_nid2sn (OpenSSL function for short name)
  - OBJ_nid2ln (OpenSSL function for long name)
  - BIO_printf (OpenSSL BIO printf function)
  - ASN1_STRING_print_ex (OpenSSL ASN.1 string printing)
  - BIO_write (OpenSSL BIO write function)
  - BIO_get_mem_data (OpenSSL BIO data extraction)
  - BIO_free (OpenSSL BIO cleanup)
  - pg_any_to_server (PostgreSQL encoding conversion)
  - pstrdup (PostgreSQL string duplication)
  - pfree (PostgreSQL memory deallocation)
  - ereport/elog (PostgreSQL error reporting)
- Called from (representative examples):
  - verify_cb (certificate verification callback)
  - be_tls_get_peer_subject_name (get peer subject name)
  - be_tls_get_peer_issuer_name (get peer issuer name)

## Notes and Other Information
- Static function - only accessible within the be-secure-openssl.c compilation unit
- Uses RFC 2253 formatting with UTF-8 conversion for international character support
- Employs BIO as an efficient string building mechanism to handle dynamic string length
- Handles encoding conversion to ensure compatibility with PostgreSQL's server encoding
- Includes comprehensive error handling for memory allocation and OpenSSL operation failures
- The output format follows the standard slash-separated DN (Distinguished Name) convention
- Memory management follows PostgreSQL conventions with pstrdup() and pfree()
- Essential for certificate validation logging and client certificate authentication features