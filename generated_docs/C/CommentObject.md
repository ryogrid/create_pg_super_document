# CommentObject

## Location
src/backend/commands/comment.c: 40 - 142

## Overview
Handles the COMMENT ON SQL command by adding comments to pg_description or pg_shdescription catalog tables for various database objects.

## Definition


## Detailed Description
CommentObject is the main entry point for processing COMMENT ON SQL statements. It validates the target object, checks permissions, and routes the comment to the appropriate catalog table (pg_description for regular objects or pg_shdescription for cluster-wide objects like databases, tablespaces, and roles).

The function includes special handling for database objects during dump restoration, treating missing databases as warnings rather than errors to prevent pg_restore failures. It also enforces restrictions on column comments, allowing them only on tables, views, materialized views, composite types, foreign tables, and partitioned tables.

## Parameters / Member Variables
- : CommentStmt structure containing the parsed COMMENT ON command with object type, target object specification, and comment text

## Dependencies
- Functions called/Symbols referenced:
  - [get_database_oid](../g/get_database_oid.md): Validates database existence
  - [get_object_address](../g/get_object_address.md): Resolves object specification to ObjectAddress
  - [check_object_ownership](../c/check_object_ownership.md): Verifies user has permission to comment on object
  - [CreateComments](CreateComments.md): Adds comment to pg_description for regular objects
  - [CreateSharedComments](CreateSharedComments.md): Adds comment to pg_shdescription for cluster-wide objects
  - [relation_close](../r/relation_close.md): Closes relation if opened during object resolution
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md): Provides error details for unsupported relation kinds
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): Main utility command dispatcher
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Secondary utility command processor
  - [ATExecCmd](../A/ATExecCmd.md): ALTER TABLE command execution

## Notes and Other Information
- Special case handling for database comments during dump restoration prevents errors from old database names
- Column comments are restricted to specific relation kinds to avoid issues with index column naming changes
- Acquires ShareUpdateExclusiveLock on target objects to prevent concurrent modifications
- Retains locks until transaction commit even after closing relations for concurrency safety