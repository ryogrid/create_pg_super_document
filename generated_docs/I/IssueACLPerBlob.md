# IssueACLPerBlob

## Location
src/bin/pg_dump/pg_backup_db.c: 599 - 672

## Overview
IssueACLPerBlob processes "LARGE OBJECTS" ACL TocEntries by parsing GRANT/REVOKE commands and applying them to all large objects listed in the associated BLOB METADATA entry.

## Definition


## Detailed Description
This function handles the restoration of Access Control List (ACL) permissions for large objects in pg_dump/pg_restore. To optimize dump file size, the TocEntry contains only one copy of the GRANT/REVOKE commands written for the first blob in a group. The function expands these commands to apply to all large objects listed in the corresponding BLOB METADATA entry.

The function performs sophisticated parsing of SQL commands to extract the structure:
1. Locates the "LARGE OBJECT" keyword to separate command prefix from suffix
2. Handles double-quoted role names by tracking quote state
3. Splits commands at semicolons to process each command separately
4. Calls IssueCommandPerBlob to apply each parsed command to all relevant large objects

This approach ensures that ACL commands are efficiently stored while being correctly applied to all target large objects during restoration.

## Parameters / Member Variables
- : Archive handle containing database connection and restoration context
- : TocEntry containing the ACL commands to be processed (depends on a BLOB METADATA entry)

## Dependencies
- Functions called/Symbols referenced:
  - getTocEntryByDumpId
  - pg_strdup
  - strncmp
  - isdigit
  - isspace
  - IssueCommandPerBlob
  - pg_free
- Types referenced:
  - ArchiveHandle
  - TocEntry

- Called from (representative examples):
  - _printTocEntry

## Notes and Other Information
- This function specifically handles "LARGE OBJECTS" ACL TocEntries and depends on the first dependency being a "BLOB METADATA" entry
- The parsing logic assumes well-formed SQL commands and handles double-quoted identifiers correctly
- Commands are processed in the order they appear, with updates to blobs being interleaved if multiple commands exist
- The function uses the first dependency of the TocEntry to locate the associated BLOB METADATA entry
- Error handling includes fatal errors if the required BLOB METADATA entry cannot be found
- File location: src/bin/pg_dump/pg_backup_db.c:599-672