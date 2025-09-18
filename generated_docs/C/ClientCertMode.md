# ClientCertMode

## Location
[src/include/libpq/hba.h:72-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/hba.h#L72-L73)

## Overview
ClientCertMode is an enumeration that defines the different modes for client certificate authentication in PostgreSQL's SSL connections.

## Definition
```c
typedef enum ClientCertMode
{
    clientCertOff,
    clientCertCA,
    clientCertFull,
} ClientCertMode;
```

## Detailed Description
This enum defines the various modes for handling client SSL certificates during authentication. It controls whether and how client certificates are validated:

- **clientCertOff**: Client certificates are not required or validated
- **clientCertCA**: Client certificates must be provided and validated by a Certificate Authority (CA), but the certificate common name is not checked against the database username
- **clientCertFull**: Client certificates must be provided, validated by a CA, and the certificate common name must match the database username

This enum is used in conjunction with SSL connections to provide different levels of certificate-based authentication security.

## Parameters / Member Variables
- `clientCertOff`: No client certificate validation required
- `clientCertCA`: Certificate must be CA-signed but username matching not required
- `clientCertFull`: Certificate must be CA-signed and common name must match username

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - [HbaLine](../H/HbaLine.md) (struct member)
  - [ClientAuthentication](ClientAuthentication.md)
  - [parse_hba_auth_opt](../p/parse_hba_auth_opt.md)
  - [get_hba_options](../g/get_hba_options.md)

## Notes and Other Information
- Defined in src/include/libpq/hba.h:67-72
- Used as the clientcert member of the HbaLine structure
- Only relevant for SSL/TLS connections (hostssl connection type)
- Works in combination with ClientCertName enum to specify how certificate names are validated
- Part of PostgreSQL's comprehensive SSL authentication system
- The distinction between clientCertCA and clientCertFull allows flexible certificate validation policies
- When clientCertFull is used, the certificate's Common Name (CN) must exactly match the PostgreSQL username