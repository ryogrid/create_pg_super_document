# UserMapping

## Location
src/include/foreign/foreign.h: 45 - 51

## Overview
UserMapping is a structure that represents a user mapping in PostgreSQL's FDW system, which defines the authentication and connection credentials for a specific local user when accessing a particular foreign server.

## Definition
```c
typedef struct UserMapping
{
    Oid         umid;           /* Oid of user mapping */
    Oid         userid;         /* local user Oid */
    Oid         serverid;       /* server Oid */
    List       *options;        /* useoptions as DefElem list */
} UserMapping;
```

## Detailed Description
The UserMapping structure represents the association between a PostgreSQL user and a foreign server, encapsulating the authentication credentials and connection options needed for that user to access the external data source. This structure is fundamental to the FDW security model, as it allows different PostgreSQL users to have different credentials for the same foreign server, or to have customized connection parameters. The options list typically contains sensitive information like passwords, certificates, or other authentication data.

## Parameters / Member Variables
- `umid`: The unique object identifier (OID) for this user mapping
- `userid`: The OID of the local PostgreSQL user this mapping applies to
- `serverid`: The OID of the foreign server this mapping is associated with
- `options`: A list of DefElem structures containing user-specific connection and authentication options

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - List (PostgreSQL list structure)
  - DefElem (option definition element)
- Called from (representative examples):
  - GetUserMapping
  - GetForeignServerByName

## Notes and Other Information
- This structure is defined in src/include/foreign/foreign.h and is essential for FDW authentication
- Each combination of user and server can have at most one user mapping
- Options commonly include credentials like username, password, or authentication certificates for the remote system
- The userid can refer to a specific user or to PUBLIC (meaning all users)
- User mappings are checked during foreign table access to determine authentication parameters
- Security-sensitive structure as it contains authentication credentials
- Used by FDW handlers to establish connections to remote data sources with proper authentication