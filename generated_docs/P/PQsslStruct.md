# PQsslStruct

## Location
[src/interfaces/libpq/fe-secure-openssl.c:1804-1813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L1804-L1813)

## Overview
Returns a pointer to an SSL-related structure by name, providing a generic interface for accessing SSL implementation-specific objects associated with a PostgreSQL connection.

## Definition

```c
void *
PQsslStruct(PGconn *conn, const char *struct_name)
```
## Detailed Description
PQsslStruct is a public API function in PostgreSQL's libpq library that provides a generic mechanism for accessing SSL implementation-specific structures. Unlike PQgetssl which directly returns the SSL object, this function allows for extensibility by accepting a structure name parameter.

Currently, the function only recognizes the string "OpenSSL" as a valid structure name, in which case it returns the same SSL object that PQgetssl would return. This design allows for potential future support of other SSL implementations while maintaining API compatibility.

The function is designed to be SSL implementation-agnostic, providing a way for applications to query for specific SSL structures without needing to know the underlying SSL library details at compile time.

## Parameters / Member Variables
- : Pointer to the PostgreSQL connection object (PGconn structure)
- : String identifying the requested SSL structure type (currently only "OpenSSL" is supported)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function for string comparison)
  - conn->ssl (direct member access to SSL object in PGconn structure)
- Called from (representative examples):
  - External applications using libpq
  - References found in libpq-fe.h:409

## Notes and Other Information
- Returns NULL if the connection parameter is NULL
- Returns NULL if struct_name is not recognized (currently only "OpenSSL" is supported)
- Returns the SSL object pointer when struct_name is "OpenSSL"
- The function is declared in the public libpq-fe.h header, making it part of the official libpq API
- This provides a more flexible alternative to PQgetssl for future SSL library support
- The design suggests potential future support for other SSL implementations
- Located in src/interfaces/libpq/fe-secure-openssl.c:1804-1813