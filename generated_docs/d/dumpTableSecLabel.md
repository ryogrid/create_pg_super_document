# dumpTableSecLabel

## Location
src/bin/pg_dump/pg_dump.c: 15470 - 15551

## Overview
Dumps security labels for both a specified table (or view) and its columns to the archive, generating SECURITY LABEL statements as part of the pg_dump output.

## Definition


## Detailed Description
This function is responsible for extracting and dumping security labels associated with a table or view and its columns. It searches for security labels using the table's catalog information, then generates appropriate SECURITY LABEL SQL statements for restoration. The function handles both table-level and column-level security labels, distinguishing between them using the objsubid field. Security labels are metadata used by security modules like SELinux to provide mandatory access control policies.

The function respects dump options, skipping execution if --no-security-labels is specified or if only data (not schema) is being dumped. For each security label found, it constructs the appropriate SQL statement and adds it to the archive as a separate entry with dependencies on the table object.

## Parameters / Member Variables
- : Archive structure for output, containing dump options and formatting functions
- : TableInfo structure containing metadata about the table including catalog ID, name, namespace, and owner
- : String describing the relation type (e.g., "TABLE", "VIEW") for use in generated SQL statements

## Dependencies
- Functions called/Symbols referenced:
  - [findSecLabels](../f/findSecLabels.md): Searches for security labels associated with the table
  - createPQExpBuffer: Creates buffer for SQL statement construction
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md): Resets buffer contents for reuse
  - fmtQualifiedDumpable: Formats qualified object name for SQL
  - [getAttrName](../g/getAttrName.md): Retrieves column name by attribute number
  - [fmtId](../f/fmtId.md): Formats identifier with proper quoting
  - appendStringLiteralAH: Appends properly escaped string literal
  - [createDumpId](../c/createDumpId.md): Creates unique dump identifier
  - [ArchiveEntry](../A/ArchiveEntry.md): Adds entry to archive with dependencies
  - destroyPQExpBuffer: Cleans up allocated buffers
- Called from:
  - [dumpTableSchema](dumpTableSchema.md): Called to dump security labels as part of table schema dumping

## Notes and Other Information
- Security labels are schema objects, not data, so they are skipped when dataOnly option is set
- The function handles both table-level (objsubid == 0) and column-level (objsubid > 0) security labels
- Each security label entry becomes a separate archive entry with proper dependencies
- Uses proper SQL escaping and identifier formatting to handle special characters in names
- Memory management is handled through PQExpBuffer creation and destruction
- Part of PostgreSQL's pg_dump utility for database backup and restoration