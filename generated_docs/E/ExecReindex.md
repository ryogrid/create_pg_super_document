# ExecReindex

## Location
src/backend/commands/indexcmds.c: 2693 - 2787

## Overview
ExecReindex is the primary entry point for manual REINDEX commands, serving as a preparation wrapper that parses options and delegates to appropriate subroutines based on the type of object being reindexed.

## Definition


## Detailed Description
ExecReindex processes REINDEX statements by parsing command options (verbose, concurrently, tablespace), validating permissions, and dispatching to the appropriate reindex function based on the target object type. It handles five types of reindex operations:
- REINDEX_OBJECT_INDEX: Single index reindexing via ReindexIndex
- REINDEX_OBJECT_TABLE: Table reindexing via ReindexTable  
- REINDEX_OBJECT_SCHEMA: Schema reindexing via ReindexMultipleTables
- REINDEX_OBJECT_SYSTEM: System catalog reindexing via ReindexMultipleTables
- REINDEX_OBJECT_DATABASE: Database reindexing via ReindexMultipleTables

The function enforces transaction block restrictions for concurrent operations and multi-object reindexing, and validates tablespace permissions when a target tablespace is specified.

## Parameters / Member Variables
- : ParseState for error reporting with location information
- : ReindexStmt containing the parsed REINDEX command details
- : Boolean indicating if this is a top-level command (affects transaction block checking)

## Dependencies
- Functions called/Symbols referenced:
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md) (prevents execution in transaction blocks for certain operations)
  - [defGetBoolean](../d/defGetBoolean.md), defGetString (option parsing functions)
  - [get_tablespace_oid](../g/get_tablespace_oid.md) (tablespace name to OID conversion)
  - [object_aclcheck](../o/object_aclcheck.md), aclcheck_error (permission checking)
  - [ReindexIndex](../R/ReindexIndex.md) (single index reindexing)
  - [ReindexTable](../R/ReindexTable.md) (table reindexing)
  - [ReindexMultipleTables](../R/ReindexMultipleTables.md) (schema/system/database reindexing)
  - Various REINDEXOPT_* and REINDEX_OBJECT_* constants
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1567)

## Notes and Other Information
- This is a public function declared in defrem.h
- Supports three main options: VERBOSE, CONCURRENTLY, and TABLESPACE
- REINDEX CONCURRENTLY cannot run inside transaction blocks due to its multi-transaction nature
- Schema, system catalog, and database reindexing operations also cannot run in transaction blocks
- When specifying a tablespace, the function validates CREATE permissions on the target tablespace
- The function builds a ReindexParams structure to pass configuration to the actual reindex implementations
- Error handling includes both syntax errors for invalid options and permission errors for tablespace access