# CreateTableSpace

## Location
src/backend/commands/tablespace.c: 208 - 394

## Overview
Creates a new tablespace by validating parameters, inserting catalog entries, creating filesystem directories, and logging the operation in WAL, with strict permission checks and validation.

## Definition


## Detailed Description
CreateTableSpace implements the CREATE TABLESPACE SQL command, handling the complete process of tablespace creation. The function performs extensive validation including superuser privilege checks, path validation, name collision detection, and filesystem structure creation.

The process involves multiple phases: parameter validation and canonicalization, catalog insertion with proper locking to prevent race conditions, filesystem directory creation, WAL logging for crash recovery, and dependency tracking. The function uses forced synchronous commit to minimize the window between filesystem changes and transaction commit.

Special handling is provided for binary upgrade scenarios and in-place tablespaces (developer feature). The function integrates with PostgreSQL's object management system through dependency recording and post-creation hooks.

## Parameters / Member Variables
- : CreateTableSpaceStmt structure containing tablespace name, location, owner specification, and options

## Dependencies
- Functions called/Symbols referenced:
  - superuser: Checks if current user has superuser privileges
  - get_rolespec_oid: Resolves role specification to OID
  - canonicalize_path: Normalizes filesystem path
  - is_absolute_path: Validates path is absolute
  - IsReservedName: Checks for reserved name patterns
  - get_tablespace_oid: Checks for existing tablespace with same name
  - GetNewOidWithIndex: Allocates new OID for tablespace
  - transformRelOptions: Processes tablespace options
  - tablespace_reloptions: Validates tablespace-specific options
  - heap_form_tuple: Creates catalog tuple
  - CatalogTupleInsert: Inserts tuple into system catalog
  - recordDependencyOnOwner: Records ownership dependency
  - create_tablespace_directories: Creates filesystem structure
  - XLogBeginInsert, XLogRegisterData, XLogInsert: WAL logging functions
  - ForceSyncCommit: Forces synchronous transaction commit
- Called from (representative examples):
  - standard_ProcessUtility: During SQL command processing

## Notes and Other Information
- Requires superuser privileges for execution
- Performs comprehensive path validation including length checks and security restrictions
- Warns against creating tablespaces within the data directory
- Reserves 'pg_' prefix for system tablespaces
- Uses binary upgrade OID override when in binary upgrade mode
- Implements double-checked locking pattern for name collision detection
- Forces synchronous commit to ensure atomicity between filesystem and catalog changes
- Integrates with PostgreSQL's dependency system and object creation hooks