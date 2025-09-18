# dumpCommentExtended

## Location
src/bin/pg_dump/pg_dump.c: 10146 - 10245

## Overview
Dumps comments associated with database objects by searching for matching pg_description entries and generating COMMENT ON statements in the dump output.

## Definition


## Detailed Description
This function is responsible for dumping comments for database objects during a pg_dump operation. It searches the pg_description catalog for comments matching the specified catalogId and subid, then generates appropriate COMMENT ON SQL statements. The function handles special cases such as:

- Large Object comments (treated as data rather than schema)
- initdb-created comments (skipped to avoid complications for non-superuser dumps)
- Proper dependency tracking in the dump file
- Section placement (marked as SECTION_NONE to belong with parent object)

The function respects dump options like --no-comments, --data-only, and --schema-only to determine whether comments should be included in the output.

## Parameters / Member Variables
- : Archive context for the dump operation
- : Object type string (e.g., "TABLE", "FUNCTION", "TRIGGER name ON")
- : Object name ready for printing (without schema decoration)
- : Schema namespace of the object for labeling
- : Owner of the object for labeling
- : Catalog identifier (tableoid and oid) for pg_description lookup
- : Sub-object identifier for pg_description lookup (0 for main object)
- : Dump ID for dependency tracking in the output
- : Expected comment text created by initdb (NULL if none)

## Dependencies
- Functions called/Symbols referenced:
  - findComments
  - fmtId
  - appendStringLiteralAH
  - createDumpId
  - ArchiveEntry
  - ARCHIVE_OPTS
- Called from (representative examples):
  - dumpComment
  - dumpNamespace

## Notes and Other Information
- Comments are marked as SECTION_NONE so they appear in the same section as their parent object
- The routine should be called immediately after calling ArchiveEntry() for the associated object
- Large Object comments are treated as data, not schema, for dump filtering purposes
- Special handling for initdb comments prevents dumping system-provided comments that would complicate non-superuser usage
- The function handles cases where initdb comments have been removed by the DBA