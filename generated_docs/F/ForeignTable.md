# ForeignTable

## Location
src/include/foreign/foreign.h: 53 - 58

## Overview
ForeignTable is a structure that represents a foreign table in PostgreSQL's FDW system, which defines a table that maps to data stored in an external data source through a foreign server.

## Definition
```c
typedef struct ForeignTable
{
    Oid         relid;          /* relation Oid */
    Oid         serverid;       /* server Oid */
    List       *options;        /* ftoptions as DefElem list */
} ForeignTable;
```

## Detailed Description
The ForeignTable structure represents a foreign table definition in PostgreSQL, which provides a local view of data stored in an external system. It connects a PostgreSQL relation (table) to a specific foreign server, along with table-specific options that control how the data is accessed and mapped. This structure is the final layer in the FDW hierarchy, where actual data access operations are configured and executed through the associated foreign data wrapper and server.

## Parameters / Member Variables
- `relid`: The OID of the PostgreSQL relation (table) that represents this foreign table
- `serverid`: The OID of the foreign server through which this table's data is accessed
- `options`: A list of DefElem structures containing table-specific options such as schema name, table name, or other FDW-specific configuration

## Dependencies
- Functions called/Symbols referenced:
  - Oid (built-in type)
  - [List](../L/List.md) (PostgreSQL list structure)
  - [DefElem](../D/DefElem.md) (option definition element)
- Called from (representative examples):
  - [GetForeignTable](../G/GetForeignTable.md)
  - [GetUserMapping](../G/GetUserMapping.md)

## Notes and Other Information
- This structure is defined in src/include/foreign/foreign.h and represents the final mapping layer in the FDW system
- The relid links the foreign table to its corresponding PostgreSQL table definition in the system catalogs
- Options typically include the remote schema name, remote table name, and other table-specific parameters
- Each foreign table is associated with exactly one foreign server, which determines the connection and FDW handler
- The actual table schema (column definitions, types) is stored in the standard PostgreSQL relation catalogs
- Used by FDW handlers to determine how to map between PostgreSQL table operations and remote data source operations
- Represents the user-visible interface for accessing external data as if it were a local PostgreSQL table