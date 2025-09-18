# psql_start_command

## Location
src/test/regress/pg_regress.c: 1115 - 1126

## Overview
Initiates a psql command string by creating a StringInfo buffer containing the base psql invocation with standard options for regression testing.

## Definition
```c
static StringInfo psql_start_command(void)
```

## Detailed Description
This function is part of a three-function suite (psql_start_command, psql_add_command, psql_end_command) designed to build and execute psql commands during PostgreSQL regression testing. It creates the foundation of a psql command string by initializing a StringInfo buffer and populating it with the psql executable path and standard command-line options.

The function constructs a command that includes the full path to the psql binary (if bindir is set) and adds the -X and -q flags. The -X flag prevents psql from reading startup files, and -q enables quiet mode to reduce output verbosity during testing.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo
  - appendStringInfo
  - bindir (global variable)
- Called from (representative examples):
  - psql_command
  - [drop_database_if_exists](../d/drop_database_if_exists.md)  
  - [create_database](../c/create_database.md)
  - [drop_role_if_exists](../d/drop_role_if_exists.md)
  - [create_role](../c/create_role.md)

## Notes and Other Information
- This function is designed to work in conjunction with psql_add_command() and psql_end_command() to build complete psql command strings
- The returned StringInfo buffer should be further populated with SQL commands using psql_add_command() before being executed with psql_end_command()
- The bindir global variable determines whether to include a full path to the psql executable or rely on PATH resolution
- Uses double quotes around the psql path to handle paths containing spaces
- The -X and -q flags ensure consistent, minimal output suitable for automated testing environments