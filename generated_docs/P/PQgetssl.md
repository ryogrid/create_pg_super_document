# PQgetssl

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1796-1803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1796-L1803)

## Overview
Returns a pointer to the OpenSSL object (SSL structure) associated with a PostgreSQL connection, allowing direct access to SSL-specific functionality.

## Definition


## Detailed Description
PQgetssl is a public API function in PostgreSQL's libpq library that provides access to the underlying OpenSSL SSL object for an established connection. This function allows applications to inspect or manipulate SSL-specific properties that are not directly exposed through other libpq functions.

The function returns a void pointer to maintain API compatibility, but the actual object is an OpenSSL SSL structure when OpenSSL support is compiled in. The returned pointer can be safely cast to SSL* when working with OpenSSL functions directly.

This is part of the SSL information functions suite in libpq, designed to give applications low-level access to SSL connection details when needed for advanced SSL configuration or debugging purposes.

## Parameters / Member Variables
- : Pointer to the PostgreSQL connection object (PGconn structure)

## Dependencies
- Functions called/Symbols referenced:
  - conn->ssl (direct member access to SSL object in PGconn structure)
- Called from (representative examples):
  - External applications using libpq
  - References found in libpq-fe.h:415

## Notes and Other Information
- Returns NULL if the connection parameter is NULL
- Returns the raw SSL object pointer stored in the connection structure  
- The function is declared in the public libpq-fe.h header, making it part of the official libpq API
- Applications using this function should be prepared to handle OpenSSL-specific data types
- The SSL object is only valid when an SSL connection has been established
- Located in src/interfaces/libpq/fe-secure-openssl.c:1796-1803