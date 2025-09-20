# TRI_YES

## Location
[src/bin/psql/settings.h:77-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/settings.h#L77-L79)

## Overview
TRI_YES is an enumeration constant representing the "yes" or "true" state in PostgreSQL's three-valued logic system, used throughout PostgreSQL client utilities for configuration options that can have default, yes, or no values.

## Definition

```c
enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES,
};
```
## Detailed Description
TRI_YES is part of the trivalue enumeration which implements a three-state boolean logic system commonly used in PostgreSQL client applications. This enumeration provides a way to distinguish between an explicitly set "yes" value, an explicitly set "no" value, and a default/unset state where the system should use its default behavior.

The trivalue system is essential in PostgreSQL client tools because many configuration options need to support inheritance from parent contexts or default values. For example, a connection parameter might be unset (TRI_DEFAULT) at the user level but have a system default, explicitly disabled (TRI_NO), or explicitly enabled (TRI_YES).

This enumeration pattern appears in multiple PostgreSQL client utilities including pg_dump, psql, and various administrative scripts, providing consistent three-valued logic semantics across the PostgreSQL ecosystem.

## Parameters / Member Variables
- : Represents the default/unset state (typically value 0)
- : Represents an explicit "no" or "false" state (typically value 1) 
- : Represents an explicit "yes" or "true" state (typically value 2)

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration constant)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck, pg_dump, pg_dumpall, pg_restore, psql, clusterdb, createdb, createuser, dropdb, dropuser, reindexdb, vacuumdb)
  - [ConnectDatabase](../C/ConnectDatabase.md) (in pg_backup_db.c)
  - [exec_command_connect](../e/exec_command_connect.md) (in psql/command.c)
  - [do_connect](../d/do_connect.md) (in psql/command.c)
  - [parse_psql_options](../p/parse_psql_options.md) (in psql/startup.c)
  - [connectDatabase](../c/connectDatabase.md) (in fe_utils/connect_utils.c)

## Notes and Other Information
The TRI_YES constant is widely used across PostgreSQL's client utilities for handling command-line options and configuration parameters that need three-state logic. Common use cases include connection parameters (force password prompts), output formatting options, and behavioral flags where "use default behavior" is a meaningful third state alongside explicit enable/disable.

The enumeration is defined identically in multiple header files (pg_backup.h and settings.h) as it's needed across different PostgreSQL client components. This design allows each component to use the enumeration without requiring shared dependencies while maintaining consistent semantics.

In practice, TRI_YES is often used in conditional statements where code checks for explicit enablement of features, distinguishing it from both explicit disablement (TRI_NO) and default behavior (TRI_DEFAULT).