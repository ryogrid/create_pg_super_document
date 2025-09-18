# ForeignServer

## Location
src/include/foreign/foreign.h: 34 - 43

## Overview
ForeignServer is a structure that represents a foreign server in PostgreSQL, which defines a specific instance of an external data source that uses a particular foreign data wrapper.

## Definition
```c
typedef struct ForeignServer
{
    Oid         serverid;       /* server Oid */
    Oid         fdwid;          /* foreign-data wrapper */
    Oid         owner;          /* server owner user Oid */
    char       *servername;     /* name of the server */
    char       *servertype;     /* server type, optional */
    char       *serverversion;  /* server version, optional */
    List       *options;        /* srvoptions as DefElem list */
} ForeignServer;
```

## Detailed Description
The ForeignServer structure represents a configured foreign server instance in PostgreSQL's FDW system. It acts as a connection configuration that defines how to access a specific external data source through its associated foreign data wrapper. Each foreign server can have its own connection parameters, authentication settings, and other configuration options stored in the options list. The structure bridges the gap between the abstract FDW definition and the concrete external data source instance.

## Parameters / Member Variables
- `serverid`: The unique object identifier (OID) for this foreign server
- `fdwid`: The OID of the associated foreign data wrapper that handles this server
- `owner`: The OID of the user who owns this foreign server
- `servername`: The string name of the foreign server
- `servertype`: Optional string describing the type of the foreign server
- `serverversion`: Optional string describing the version of the foreign server
- `options`: A list of DefElem structures containing server-specific options and connection parameters

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - List (PostgreSQL list structure)
  - DefElem (option definition element)
- Called from (representative examples):
  - GetForeignServer
  - GetForeignServerExtended
  - CreateUserMapping
  - CreateForeignTable
  - AlterUserMapping
  - RemoveUserMapping
  - ImportForeignSchema

## Notes and Other Information
- This structure is defined in src/include/foreign/foreign.h and is central to PostgreSQL's foreign server management
- The fdwid field links the server to its corresponding foreign data wrapper
- Server type and version are optional metadata fields that can be used for documentation or FDW-specific logic
- Options typically contain connection parameters like host, port, database name, and other FDW-specific settings
- Multiple foreign tables can reference the same foreign server
- Used extensively in user mapping and foreign table operations as servers define the connection context