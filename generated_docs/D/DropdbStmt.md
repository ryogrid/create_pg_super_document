# DropdbStmt

## Location
src/include/nodes/parsenodes.h: 3800 - 3806

## Overview
A parse node structure representing the DROP DATABASE statement, used to remove a database from the PostgreSQL cluster.

## Definition

```c
typedef struct DropdbStmt
{
	NodeTag		type;
	char	   *dbname;			/* database to drop */
	bool		missing_ok;		/* skip error if db is missing? */
	List	   *options;		/* currently only FORCE is supported */
} DropdbStmt;
```
## Detailed Description
DropdbStmt is a parse node structure that represents a DROP DATABASE SQL statement. This structure is created during parsing of SQL commands like "DROP DATABASE mydb" or "DROP DATABASE IF EXISTS mydb WITH (FORCE)". The statement allows removing a database from the PostgreSQL cluster, with options to handle missing databases gracefully and force the drop even if there are active connections.

The structure captures all the necessary information for the database removal operation, including the database name, error handling preferences, and any additional options specified in the command.

## Parameters / Member Variables
- : NodeTag identifying this as a DropdbStmt node
- : String containing the name of the database to be dropped
- : Boolean flag indicating whether to skip errors if the database doesn't exist (IF EXISTS clause)
- : List of DefElem nodes containing additional options (currently only FORCE is supported)

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL list structure for options)
  - DefElem (for option definitions)
- Called from (representative examples):
  - DropDatabase (execution function in dbcommands.c)
  - standard_ProcessUtility (utility command processing)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from Node via the NodeTag
- The FORCE option allows dropping databases with active connections, terminating those connections
- The missing_ok flag corresponds to the IF EXISTS clause in the SQL statement
- Database dropping requires appropriate privileges and cannot be performed within a transaction block
- The actual database removal is handled by the dropdb function after processing the statement options