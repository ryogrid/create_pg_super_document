# ConnType

## Location
src/include/libpq/hba.h: 65 - 66

## Overview
ConnType is an enumeration that defines the different types of database connections supported by PostgreSQL's host-based authentication (HBA) system.

## Definition
```c
typedef enum ConnType
{
    ctLocal,
    ctHost,
    ctHostSSL,
    ctHostNoSSL,
    ctHostGSS,
    ctHostNoGSS,
} ConnType;
```

## Detailed Description
This enum defines the various connection types that can be specified in pg_hba.conf entries. Each value corresponds to a different method of connecting to the PostgreSQL database server:

- **ctLocal**: Unix domain socket connections (local connections)
- **ctHost**: Standard TCP/IP connections (includes both SSL and non-SSL)
- **ctHostSSL**: TCP/IP connections that must use SSL encryption
- **ctHostNoSSL**: TCP/IP connections that must not use SSL encryption
- **ctHostGSS**: TCP/IP connections that must use GSS (Generic Security Service) encryption
- **ctHostNoGSS**: TCP/IP connections that must not use GSS encryption

The enum is used to match incoming connection attempts against the appropriate authentication rules defined in the pg_hba.conf configuration file.

## Parameters / Member Variables
- `ctLocal`: Matches Unix domain socket connections
- `ctHost`: Matches general TCP/IP connections
- `ctHostSSL`: Matches SSL-encrypted TCP/IP connections only
- `ctHostNoSSL`: Matches non-SSL TCP/IP connections only
- `ctHostGSS`: Matches GSS-encrypted TCP/IP connections only
- `ctHostNoGSS`: Matches non-GSS TCP/IP connections only

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enum definition)
- Called from (representative examples):
  - HbaLine (struct member)
  - CheckPAMAuth
  - parse_hba_line
  - check_hba
  - fill_hba_line

## Notes and Other Information
- Defined in src/include/libpq/hba.h:57-65
- Used as the conntype member of the HbaLine structure
- Critical for determining which authentication rules apply to specific connection types
- Works with PostgreSQL's flexible authentication system to support various security requirements
- The distinction between SSL/NoSSL and GSS/NoGSS allows fine-grained control over connection security