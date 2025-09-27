# X509_NAME_to_cstring

## Location
[src/backend/libpq/be-secure-openssl.c:1618-1690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1618-L1690)

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
  - [pg_any_to_server](../p/pg_any_to_server.md) (PostgreSQL encoding conversion)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - ereport/elog (PostgreSQL error reporting)
- Called from (representative examples):
  - [verify_cb](../v/verify_cb.md) (certificate verification callback)
  - [be_tls_get_peer_subject_name](../b/be_tls_get_peer_subject_name.md) (get peer subject name)
  - [be_tls_get_peer_issuer_name](../b/be_tls_get_peer_issuer_name.md) (get peer issuer name)

## Notes and Other Information
- Static function - only accessible within the be-secure-openssl.c compilation unit
- Uses RFC 2253 formatting with UTF-8 conversion for international character support
- Employs BIO as an efficient string building mechanism to handle dynamic string length
- Handles encoding conversion to ensure compatibility with PostgreSQL's server encoding
- Includes comprehensive error handling for memory allocation and OpenSSL operation failures
- The output format follows the standard slash-separated DN (Distinguished Name) convention
- Memory management follows PostgreSQL conventions with pstrdup() and pfree()
- Essential for certificate validation logging and client certificate authentication features

## Simplified Source

```c
// Simplified version of X509_NAME_to_cstring
static char *X509_NAME_to_cstring(X509_NAME *name) {
    // Create memory buffer for building the string
    BIO *membuf = BIO_new(BIO_s_mem());
    if (membuf == NULL)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("could not create BIO")));

    BIO_set_close(membuf, BIO_CLOSE);

    // Iterate through all name entries in the X509 name
    int count = X509_NAME_entry_count(name);
    for (int i = 0; i < count; i++) {
        // Get name entry and extract field information
        X509_NAME_ENTRY *entry = X509_NAME_get_entry(name, i);
        int nid = OBJ_obj2nid(X509_NAME_ENTRY_get_object(entry));
        ASN1_STRING *value = X509_NAME_ENTRY_get_data(entry);

        // Get field name (short name preferred, fallback to long name)
        const char *field_name = OBJ_nid2sn(nid);
        if (field_name == NULL)
            field_name = OBJ_nid2ln(nid);

        // Error if we can't determine the field name
        if (field_name == NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("could not convert NID %d to an ASN1_OBJECT structure", nid)));

        // Format as "/fieldname=value" and add to buffer
        BIO_printf(membuf, "/%s=", field_name);
        ASN1_STRING_print_ex(membuf, value,
                            ((ASN1_STRFLGS_RFC2253 & ~ASN1_STRFLGS_ESC_MSB) |
                             ASN1_STRFLGS_UTF8_CONVERT));
    }

    // Null-terminate the buffer content
    char nullterm = '\0';
    BIO_write(membuf, &nullterm, 1);

    // Extract the built string and convert encoding
    char *buffer_data;
    size_t size = BIO_get_mem_data(membuf, &buffer_data);
    char *converted = pg_any_to_server(buffer_data, size - 1, PG_UTF8);

    // Create final result string
    char *result = pstrdup(converted);
    if (converted != buffer_data)
        pfree(converted);

    // Clean up BIO buffer
    if (BIO_free(membuf) != 1)
        elog(ERROR, "could not free OpenSSL BIO structure");

    return result;
}
```

Key simplifications made:
- Consolidated variable declarations for better readability
- Added descriptive comments for each major operation
- Simplified the loop structure and variable naming
- Grouped related operations together logically
- Removed some intermediate variables while preserving functionality
- Focused on the main execution path while keeping essential error handling