# CreateSchemaCommand

## Location
src/backend/commands/schemacmds.c: 52 - 248

## Overview
CreateSchemaCommand implements the CREATE SCHEMA SQL command, creating a new database schema and executing any embedded SQL statements within the schema creation context.

## Definition


## Detailed Description
CreateSchemaCommand orchestrates the complete CREATE SCHEMA operation, handling authorization, namespace creation, and execution of embedded statements. The function performs comprehensive security checks, creates the schema namespace, temporarily modifies the search path to include the new schema, and processes any embedded SQL statements (like CREATE TABLE, CREATE VIEW) within the schema context.

Key behaviors include:
- Resolving the schema owner (either specified or defaulting to current user)
- Performing ACL checks for database CREATE privilege and role ownership
- Handling IF NOT EXISTS logic with extension membership validation
- Creating the actual namespace through NamespaceCreate
- Temporarily prepending the new schema to search_path during embedded statement execution
- Processing embedded statements in dependency-resolved order
- Proper cleanup of security context and GUC settings

## Parameters / Member Variables
- : CreateSchemaStmt containing schema name, owner role spec, IF NOT EXISTS flag, and embedded statement list
- : Original SQL query string for error reporting and logging
- : Character offset of the statement in the query string
- : Length of the statement in characters

## Dependencies
- Functions called/Symbols referenced:
  - NamespaceCreate (creates the actual namespace)
  - GetUserIdAndSecContext/SetUserIdAndSecContext (security context management)
  - get_rolespec_oid (resolves owner role specification)
  - object_aclcheck (checks CREATE privilege on database)
  - checkMembershipInCurrentExtension (validates extension membership for IF NOT EXISTS)
  - transformCreateSchemaStmtElements (reorganizes embedded statements)
  - ProcessUtility (executes embedded statements)
  - EventTriggerCollectSimpleCommand (reports schema creation to event triggers)
- Called from (representative examples):
  - ProcessUtilitySlow (main utility command processing)
  - CreateExtensionInternal (schema creation during extension installation)

## Notes and Other Information
- Returns the OID of the created namespace, or InvalidOid if skipped due to IF NOT EXISTS
- Temporarily modifies search_path to include the new schema during embedded statement execution
- Uses function-level SET to ensure search_path changes persist only for the duration of schema creation
- Validates against reserved schema names (pg_* prefix) unless allowSystemTableMods is enabled
- Handles ownership transfer by temporarily switching user context during creation
- Location information is passed through to embedded statements for consistent error reporting