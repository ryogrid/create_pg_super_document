# _printTocEntry

## Location
src/bin/pg_dump/pg_backup_archiver.c: 3758 - 3950

## Overview
Emits the SQL commands to create the object represented by a TOC entry, including header comments, object definition, and ALTER OWNER commands for pg_dump restoration operations.

## Definition


## Detailed Description
This function is the core output generator for pg_dump's restore process. It handles the complete restoration workflow for database objects by:

1. **Setting context**: Selects appropriate owner, schema, tablespace, and access method
2. **Generating comments**: Creates descriptive header comments with object metadata and dependencies
3. **Processing definitions**: Handles three special cases:
   - Schema definitions with --no-owner mode (strips AUTHORIZATION clause)
   - BLOB METADATA entries (processes OID lists)
   - ACL LARGE OBJECTS entries (applies ACL commands to multiple objects)
4. **Owner restoration**: Issues ALTER OWNER commands when not using SET SESSION AUTH
5. **Post-processing**: Handles partitioned table access methods and ACL session cleanup

The function manages transaction counting for bulk operations by counting semicolons in SQL definitions (excluding functions/procedures) and handles various edge cases for different PostgreSQL object types.

## Parameters / Member Variables
- : ArchiveHandle pointer containing archive state and output functions
- : TocEntry pointer with object metadata (name, type, definition, owner, dependencies, etc.)
- : Boolean flag indicating whether this is a data entry (affects comment prefix)

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry, RestoreOptions (struct types)
  - _becomeOwner, _selectOutputSchema, _selectTablespace, _selectTableAccessMethod
  - ahprintf (formatted output to archive)
  - sanitize_line (comment sanitization)
  - fmtId (identifier quoting)
  - IssueCommandPerBlob, IssueACLPerBlob (special BLOB handling)
  - _getObjectDescription (object description generation)
  - initPQExpBuffer, termPQExpBuffer (buffer management)
  - _printTableAccessMethodNoStorage (partitioned table handling)
  - _tocEntryIsACL (ACL entry detection)
- Called from:
  - restore_toc_entry (main restoration function, multiple call sites)

## Notes and Other Information
- Function is static and only used within pg_backup_archiver.c
- Handles complex restore scenarios including no-owner mode, blob metadata, and ACL processing
- Uses transaction counting heuristics for bulk operations (counting semicolons)
- Special handling for schema "public" when using comment-based creation
- Manages session authorization state cleanup after ACL processing
- Supports both verbose and compact output modes through AH->public.verbose
- Integrates with tablespace and access method selection for proper object placement