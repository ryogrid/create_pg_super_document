# DropUserMappingStmt

## Location
[src/include/nodes/parsenodes.h:2924-2930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2924-L2930)

## Overview
DropUserMappingStmt is a parse node structure that represents a DROP USER MAPPING SQL statement, which removes an existing mapping between a PostgreSQL user and a foreign server.

## Definition
```c
typedef struct DropUserMappingStmt
{
    NodeTag     type;
    RoleSpec   *user;           /* user role */
    char       *servername;     /* server name */
    bool        missing_ok;     /* ignore missing mappings */
} DropUserMappingStmt;
```

## Detailed Description
DropUserMappingStmt is a parse tree node that stores the parsed representation of a DROP USER MAPPING statement. This structure is created during SQL parsing and contains the information needed to remove an existing user mapping between a PostgreSQL role and a foreign server. The statement provides a way to clean up user mappings that are no longer needed or to modify access patterns for foreign data wrapper connections.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a DropUserMappingStmt node
- `user`: RoleSpec pointer specifying the PostgreSQL user role whose mapping should be dropped
- `servername`: String containing the name of the foreign server from which to remove the user mapping
- `missing_ok`: Boolean flag indicating whether to silently ignore attempts to drop non-existent mappings (IF EXISTS clause)

## Dependencies
- Functions called/Symbols referenced:
  - RoleSpec
  - NodeTag
- Called from (representative examples):
  - RemoveUserMapping (src/backend/commands/foreigncmds.c:1335)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1611)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from the Node structure via NodeTag
- The missing_ok flag implements the IF EXISTS clause functionality, allowing graceful handling of attempts to drop non-existent mappings
- User mappings are stored in the pg_user_mapping system catalog and this statement removes entries from it
- Unlike the CREATE and ALTER variants, this structure does not contain an options list since it only removes existing mappings
- This is defined in src/include/nodes/parsenodes.h:2924-2930