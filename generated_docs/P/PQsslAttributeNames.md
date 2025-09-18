# PQsslAttributeNames

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1814-1840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1814-L1840)

## Overview
Returns an array of supported SSL attribute names that can be queried for a PostgreSQL connection, providing metadata about available SSL connection properties.

## Definition


## Detailed Description
PQsslAttributeNames is a public API function in PostgreSQL's libpq library that returns a NULL-terminated array of string constants representing the names of SSL attributes that can be queried using PQsslAttribute. This function provides introspection capabilities for applications that need to discover what SSL information is available.

The function returns different attribute arrays depending on the connection state:
- When conn is NULL: Returns the default SSL library attributes (OpenSSL attributes)
- When conn->ssl is NULL (no SSL): Returns an empty array containing only NULL
- When SSL is active: Returns the full OpenSSL attributes array

The supported OpenSSL attributes include: "library", "key_bits", "cipher", "compression", "protocol", and "alpn". This allows applications to programmatically discover and query SSL connection properties without hardcoding attribute names.

## Parameters / Member Variables
- : Pointer to the PostgreSQL connection object (PGconn structure), or NULL to get default attribute names

## Dependencies
- Functions called/Symbols referenced:
  - openssl_attrs (static array of OpenSSL attribute names)
  - empty_attrs (static array with only NULL)
  - conn->ssl (SSL object in PGconn structure for connection state check)
- Called from (representative examples):
  - External applications using libpq
  - References found in libpq-fe.h:411

## Notes and Other Information
- Returns a static array, so the returned pointer remains valid throughout program execution
- The array is NULL-terminated for easy iteration
- When connection is NULL, returns attributes supported by the default SSL implementation
- Returns empty array (only NULL) for unencrypted connections
- Supports the following SSL attributes: library, key_bits, cipher, compression, protocol, alpn
- The function is declared in the public libpq-fe.h header, making it part of the official libpq API
- Designed to work with PQsslAttribute function for querying specific attribute values
- Located in src/interfaces/libpq/fe-secure-openssl.c:1814-1840