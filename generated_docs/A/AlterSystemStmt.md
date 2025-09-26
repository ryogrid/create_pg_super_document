# AlterSystemStmt

## Location
src/include/nodes/parsenodes.h: 3812 - 3816

## Overview
A parse node structure representing the ALTER SYSTEM statement, used to modify PostgreSQL configuration parameters persistently across server restarts.

## Definition

```c
typedef struct AlterSystemStmt
{
	NodeTag		type;
	VariableSetStmt *setstmt;	/* SET subcommand */
} AlterSystemStmt;
```
## Detailed Description
AlterSystemStmt is a parse node structure that represents an ALTER SYSTEM SQL statement. This structure is created during parsing of SQL commands like "ALTER SYSTEM SET shared_buffers = '256MB'" or "ALTER SYSTEM RESET ALL". The ALTER SYSTEM command allows superusers and users with appropriate parameter-level privileges to modify server configuration parameters that persist across PostgreSQL server restarts by writing them to the postgresql.auto.conf file.

Unlike regular SET commands that affect only the current session, ALTER SYSTEM changes are written to the automatic configuration file and take effect after a configuration reload or server restart, depending on the parameter's context.

## Parameters / Member Variables
- : NodeTag identifying this as an AlterSystemStmt node
- : Pointer to a VariableSetStmt structure containing the SET or RESET operation details

## Dependencies
- Functions called/Symbols referenced:
  - VariableSetStmt (embedded structure for SET/RESET details)
- Called from (representative examples):
  - AlterSystemSetConfigFile (execution function in guc.c)
  - standard_ProcessUtility (utility command processing)
  - EmitWarningsOnPlaceholders (warning system)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from Node via the NodeTag
- ALTER SYSTEM commands write to postgresql.auto.conf file, which is automatically included in postgresql.conf
- Requires superuser privileges or specific parameter-level ALTER SYSTEM privileges
- Changes take effect after pg_reload_conf() or server restart, depending on the parameter context
- The command is not transactional - changes are immediately written to disk
- RESET ALL variant removes all settings from postgresql.auto.conf
- Parameters that cannot be set in configuration files are rejected by ALTER SYSTEM