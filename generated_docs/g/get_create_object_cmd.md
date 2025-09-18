# get_create_object_cmd

## Location
src/bin/psql/command.c: 5666 - 5825

## Overview
Constructs complete "CREATE OR REPLACE" SQL commands for PostgreSQL database objects by retrieving their definitions from the system catalogs.

## Definition


## Detailed Description
The  function generates complete DDL (Data Definition Language) statements for PostgreSQL database objects that can be used to recreate them. It supports functions and views, handling the complexity of reconstructing proper CREATE OR REPLACE statements from system catalog information.

For functions, it uses pg_get_functiondef() which returns a complete CREATE OR REPLACE FUNCTION statement. For views, it performs more complex processing: it retrieves the view definition using pg_get_viewdef(), constructs the proper CREATE OR REPLACE VIEW prefix with schema-qualified names, handles reloptions (storage parameters), and processes CHECK OPTION settings. The function also includes version-specific handling for PostgreSQL 9.4+ features like LOCAL/CASCADED CHECK OPTION.

## Parameters / Member Variables
- : EditableObjectType enum specifying the type of object (EditableFunction, EditableView)
- : Object Identifier of the database object to retrieve
- : PQExpBuffer to store the resulting CREATE OR REPLACE statement

## Dependencies
- Functions called/Symbols referenced:
  - EditableObjectType (enum defining supported object types)
  - EditableFunction, EditableView (enum values for different object types)
  - printfPQExpBuffer (formats SQL queries for system catalog lookups)
  - echo_hidden_command (displays query if ECHO_HIDDEN is enabled)
  - PQexec (executes the catalog query)
  - PGRES_TUPLES_OK (PostgreSQL result status constant)
  - resetPQExpBuffer (clears the output buffer)
  - RELKIND_VIEW, RELKIND_MATVIEW (constants for relation types)
  - fmtId (formats identifiers with proper quoting)
  - appendReloptionsArray (processes view storage parameters)
  - standard_strings (determines string literal handling)
  - minimal_error_message (displays error information)
- Called from (representative examples):
  - exec_command_ef_ev (\ef and \ev commands for editing objects)
  - exec_command_sf_sv (\sf and \sv commands for showing object definitions)

## Notes and Other Information
- Handles PostgreSQL version differences (9.4+ CHECK OPTION support)
- For views, validates that the object is actually a view (not a table or other relation type)
- Processes view-specific features: reloptions, CHECK OPTION (LOCAL/CASCADED)
- Removes trailing semicolons from pg_get_viewdef() output for consistency
- Ensures output ends with a newline for proper formatting
- Does not currently support materialized views for CREATE OR REPLACE (marked with #ifdef NOT_USED)
- Fully qualifies view names to prevent ambiguity during recreation
- Essential for psql's object editing infrastructure, enabling users to modify and recreate database objects