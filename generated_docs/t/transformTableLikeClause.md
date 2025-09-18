# transformTableLikeClause

## Location
src/backend/parser/parse_utilcmd.c: 980 - 1168

## Overview
Processes LIKE clauses in CREATE TABLE statements, copying column definitions and optionally other attributes from an existing table or view to the new table being created.

## Definition


## Detailed Description
transformTableLikeClause implements PostgreSQL's LIKE clause functionality for CREATE TABLE statements. The LIKE clause allows creating a new table with the same column structure as an existing table, view, materialized view, composite type, foreign table, or partitioned table. The function handles two phases of LIKE processing:

**Immediate processing**: 
- Copies basic column definitions (name, type, NOT NULL constraint)
- Handles immediately copyable attributes based on LIKE options (STORAGE, COMPRESSION, COMMENTS, IDENTITY, GENERATED)
- Validates permissions and relation types
- Creates necessary sequences for identity columns

**Deferred processing**: 
- For complex attributes that depend on final column numbering (DEFAULTS, CONSTRAINTS, INDEXES, STATISTICS), the function defers processing by adding the TableLikeClause to cxt->likeclauses
- These will be processed later by expandTableLikeClause() after the table structure is finalized

The function performs comprehensive permission checking, ensuring the user has appropriate access to the source relation. For composite types, it requires USAGE privilege; for other relation types, it requires SELECT privilege.

## Parameters / Member Variables
- : CreateStmtContext containing parsing state and accumulating lists for the new table
- : TableLikeClause specifying the source relation and copy options

## Dependencies
- Functions called/Symbols referenced:
  - [relation_openrv](../r/relation_openrv.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - makeColumnDef
  - [getIdentitySequence](../g/getIdentitySequence.md)
  - [sequence_options](../s/sequence_options.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)
  - [GetComment](../G/GetComment.md)
  - [GetCompressionMethodName](../G/GetCompressionMethodName.md)
  - CompressionMethodIsValid
  - makeNode (CommentStmt)
  - [makeString](../m/makeString.md)
  - list_make3
  - table_close
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md)

## Notes and Other Information
The function maintains an AccessShareLock on the source relation until transaction commit to prevent concurrent modifications that could affect the LIKE operation. Foreign tables cannot use LIKE clauses due to their external nature. The LIKE options control which aspects are copied: INCLUDING DEFAULTS, INCLUDING CONSTRAINTS, INCLUDING INDEXES, INCLUDING STORAGE, INCLUDING COMMENTS, INCLUDING IDENTITY, INCLUDING GENERATED, and INCLUDING STATISTICS. Some options require deferred processing because they depend on the final column attribute numbers in the new table. The function carefully handles identity columns by extracting sequence options from the source table's identity sequence and creating a new sequence for the target table.