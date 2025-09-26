# AlterUserMappingStmt

## Location
[src/include/nodes/parsenodes.h:2916-2922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2916-L2922)

## Overview
AlterUserMappingStmt is a parse node structure that represents an ALTER USER MAPPING SQL statement, which modifies the options of an existing mapping between a PostgreSQL user and a foreign server.

## Definition
```c
typedef struct AlterUserMappingStmt
{
    NodeTag     type;
    RoleSpec   *user;           /* user role */
    char       *servername;     /* server name */
    List       *options;        /* generic options to server */
} AlterUserMappingStmt;
```

## Detailed Description
AlterUserMappingStmt is a parse tree node that stores the parsed representation of an ALTER USER MAPPING statement. This structure is created during SQL parsing and contains the information needed to modify an existing user mapping between a PostgreSQL role and a foreign server. The statement allows updating connection parameters, credentials, or other options associated with the user mapping without dropping and recreating it.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterUserMappingStmt node
- `user`: RoleSpec pointer specifying the PostgreSQL user role whose mapping is being altered
- `servername`: String containing the name of the foreign server for the mapping to alter
- `options`: List of generic options (DefElem nodes) containing the new or updated connection parameters and credentials

## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpec](../R/RoleSpec.md)
  - NodeTag
  - [List](../L/List.md)
- Called from (representative examples):
  - [AlterUserMapping](AlterUserMapping.md) (src/backend/commands/foreigncmds.c:1237)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1607)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from the Node structure via NodeTag
- The options list contains DefElem nodes that specify which options to SET or DROP
- User mappings are stored in the pg_user_mapping system catalog and this statement modifies existing entries
- Unlike CreateUserMappingStmt, this structure does not have an if_not_exists flag since it operates on existing mappings
- This is defined in src/include/nodes/parsenodes.h:2916-2922