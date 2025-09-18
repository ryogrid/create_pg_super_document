# dumpTableComment

## Location
src/bin/pg_dump/pg_dump.c: 10262 - 10359

## Overview
Dumps comments for tables/views and their columns by searching for associated pg_description entries and generating COMMENT ON statements for both the table and its individual columns.

## Definition


## Detailed Description
This function handles comment dumping specifically for table-like objects (tables, views, etc.) and extends the basic comment functionality to include column-level comments. It searches for all comments associated with the table's catalog ID and processes them based on the objsubid value:

- objsubid = 0: Comment on the table/view itself
- objsubid > 0: Comment on a specific column (objsubid corresponds to column attribute number)

The function generates appropriate COMMENT ON TABLE/VIEW or COMMENT ON COLUMN statements and creates separate archive entries for each comment found. All comments are marked as SECTION_NONE to ensure they appear in the same section as the parent table.

## Parameters / Member Variables
- : Archive context for the dump operation
- : TableInfo structure containing table metadata including name, namespace, owner, and column information
- : Relation type name ("TABLE", "VIEW", "MATERIALIZED VIEW", etc.) for use in COMMENT ON statements

## Dependencies
- Functions called/Symbols referenced:
  - findComments
  - resetPQExpBuffer
  - fmtId
  - fmtQualifiedDumpable
  - appendStringLiteralAH
  - createDumpId
  - ArchiveEntry
  - ARCHIVE_OPTS
- Called from (representative examples):
  - dumpTableSchema

## Notes and Other Information
- Respects --no-comments and --data-only dump options
- Comments are treated as schema information, not data
- Column comments are validated against the table's column count (tbinfo->numatts)
- Each comment (table and column) creates a separate archive entry with dependency on the parent table
- Uses fmtQualifiedDumpable for properly formatted qualified table names
- Column numbers in objsubid are 1-based, requiring adjustment for 0-based array access to attnames
- Handles multiple comments per table by iterating through all found comment entries