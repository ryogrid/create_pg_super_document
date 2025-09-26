# CreateUserMappingStmt

## Location
src/include/nodes/parsenodes.h: 2907 - 2914

## Overview
CreateUserMappingStmt is a parse node structure that represents a CREATE USER MAPPING SQL statement, which creates a mapping between a PostgreSQL user and a foreign server for foreign data wrapper operations.

## Definition

```c
typedef struct CreateUserMappingStmt
{
	NodeTag		type;
	RoleSpec   *user;			/* user role */
	char	   *servername;		/* server name */
	bool		if_not_exists;	/* just do nothing if it already exists? */
	List	   *options;		/* generic options to server */
} CreateUserMappingStmt;
```
## Detailed Description
CreateUserMappingStmt is a parse tree node that stores the parsed representation of a CREATE USER MAPPING statement. This structure is created during SQL parsing and contains all the necessary information to create a user mapping between a PostgreSQL role and a foreign server. User mappings are essential for foreign data wrapper functionality, as they define how local users connect to remote data sources with appropriate credentials and connection parameters.

## Parameters / Member Variables
- : NodeTag identifying this as a CreateUserMappingStmt node
- : RoleSpec pointer specifying the PostgreSQL user role for which the mapping is created
- : String containing the name of the foreign server to map to
- : Boolean flag indicating whether to silently skip creation if the mapping already exists
- : List of generic options (DefElem nodes) containing connection parameters and credentials for the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - RoleSpec
  - NodeTag
  - List
- Called from (representative examples):
  - CreateUserMapping (src/backend/commands/foreigncmds.c:1111)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1603)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from the Node structure via NodeTag
- The options list typically contains authentication credentials and connection parameters specific to the foreign data wrapper
- User mappings are stored in the pg_user_mapping system catalog
- The if_not_exists flag implements the IF NOT EXISTS clause functionality in SQL
- This is defined in src/include/nodes/parsenodes.h:2907-2914