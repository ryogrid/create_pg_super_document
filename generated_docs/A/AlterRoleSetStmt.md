# AlterRoleSetStmt

## Location
[src/include/nodes/parsenodes.h:3097-3103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3097-L3103)

## Overview
AlterRoleSetStmt is a parse tree node structure that represents ALTER ROLE ... SET/RESET statements used to configure default values for configuration parameters for specific roles and databases.

## Definition

```c
typedef struct AlterRoleSetStmt
{
	NodeTag		type;
	RoleSpec   *role;			/* role */
	char	   *database;		/* database name, or NULL */
	VariableSetStmt *setstmt;	/* SET or RESET subcommand */
} AlterRoleSetStmt;
```
## Detailed Description
AlterRoleSetStmt is a parser node structure that encapsulates information needed for ALTER ROLE ... SET and ALTER ROLE ... RESET statements in PostgreSQL. These statements allow setting default values for configuration parameters that apply to a specific role when they connect to the database, optionally restricted to a specific database.

This mechanism enables database administrators to customize the environment for individual roles, such as setting default search paths, work memory limits, or other session parameters that will be applied automatically when the role establishes a connection.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as an AlterRoleSetStmt node
- `*role`: RoleSpec pointer specifying the target role for which to set configuration defaults
- `*database`: String containing the database name to which this setting applies, or NULL for all databases
- `*setstmt`: VariableSetStmt pointer containing the actual SET or RESET command to be applied
## Dependencies
- Functions called/Symbols referenced:
  - [RoleSpec](../R/RoleSpec.md) (for role specification)
  - [VariableSetStmt](../V/VariableSetStmt.md) (for the SET/RESET command details)
  - NodeTag (for type identification)
- Called from (representative examples):
  - [AlterRoleSet](AlterRoleSet.md) (role configuration command execution)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)

## Notes and Other Information
- This structure handles both SET and RESET operations through the embedded VariableSetStmt
- When database is NULL, the configuration applies to all databases the role connects to
- When database is specified, the setting only applies when the role connects to that particular database
- The setstmt contains the actual parameter name, value, and operation type (SET/RESET)
- These settings are stored in the pg_db_role_setting system catalog
- Common use cases include setting search_path, work_mem, or other session parameters per role
- Location: src/include/nodes/parsenodes.h:3097-3103