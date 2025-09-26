# CreateTableSpaceStmt

## Location
[src/include/nodes/parsenodes.h:2780-2787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2780-L2787)

## Overview
CreateTableSpaceStmt is a parse tree node structure that represents a CREATE TABLESPACE statement, used to define a new tablespace with specified location, owner, and configuration options.

## Definition
```c
typedef struct CreateTableSpaceStmt
{
    NodeTag     type;
    char       *tablespacename;
    RoleSpec   *owner;
    char       *location;
    List       *options;
} CreateTableSpaceStmt;
```

## Detailed Description
CreateTableSpaceStmt represents the parsed form of a CREATE TABLESPACE command in PostgreSQL. Tablespaces allow database administrators to define locations in the file system where database objects can be stored, providing control over the physical storage layout of the database.

This structure captures all the essential information needed to create a new tablespace: the tablespace name, the owning role, the filesystem location where the tablespace data will be stored, and any additional options that control tablespace behavior.

The statement is processed by the tablespace management subsystem, which handles the creation of the necessary directory structures and catalog entries to establish the new tablespace.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateTableSpaceStmt node type
- `tablespacename`: String containing the name of the tablespace to create
- `owner`: RoleSpec specifying the role that will own the tablespace (may be NULL for current user)
- `location`: String specifying the filesystem directory path where the tablespace will store data
- `options`: List of tablespace options from the WITH clause (implementation-specific parameters)

## Dependencies
- Functions called/Symbols referenced:
  - RoleSpec (for owner specification)

- Called from (representative examples):
  - CreateTableSpace (src/backend/commands/tablespace.c:208)
  - standard_ProcessUtility (src/backend/tcop/utility.c:713)

## Notes and Other Information
- Tablespaces provide a way to control the physical storage layout of PostgreSQL databases
- The location path must be an absolute path and must be accessible by the PostgreSQL server process
- Tablespace creation requires superuser privileges due to filesystem access requirements
- The owner field can be NULL, in which case the tablespace is owned by the user executing the command
- Options may include parameters specific to the underlying storage system or access method
- Created tablespaces can be used in CREATE TABLE, CREATE INDEX, and other DDL commands via the TABLESPACE clause
- The tablespace directory must be empty and owned by the PostgreSQL system user