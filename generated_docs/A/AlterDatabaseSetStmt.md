# AlterDatabaseSetStmt

## Location
[src/include/nodes/parsenodes.h:3789-3794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3789-L3794)

## Overview
A parse node structure representing the ALTER DATABASE SET statement, used to alter configuration parameters for a specific database.

## Definition

```c
typedef struct AlterDatabaseSetStmt
{
	NodeTag		type;
	char	   *dbname;			/* database name */
	VariableSetStmt *setstmt;	/* SET or RESET subcommand */
} AlterDatabaseSetStmt;
```
## Detailed Description
AlterDatabaseSetStmt is a parse node structure that represents an ALTER DATABASE SET SQL statement. This structure is created during parsing of SQL commands like "ALTER DATABASE mydb SET work_mem = '256MB'" or "ALTER DATABASE mydb RESET ALL". The statement allows setting or resetting configuration parameters for a specific database, which will affect all future sessions connecting to that database.

The structure contains the essential information needed to execute the database parameter modification: the target database name and the specific SET or RESET operation to be performed.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AlterDatabaseSetStmt node
- `*dbname`: String containing the name of the target database to modify
- `*setstmt`: Pointer to a VariableSetStmt structure containing the SET or RESET operation details
## Dependencies
- Functions called/Symbols referenced:
  - [VariableSetStmt](../V/VariableSetStmt.md) (embedded structure for SET/RESET details)
- Called from (representative examples):
  - [AlterDatabaseSet](AlterDatabaseSet.md) (execution function in dbcommands.c)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from Node via the NodeTag
- The actual execution of the ALTER DATABASE SET command is handled by the AlterDatabaseSet function
- Database-level parameter settings override server-level defaults for sessions connecting to that database
- Access control checks ensure only database owners can modify database-level settings