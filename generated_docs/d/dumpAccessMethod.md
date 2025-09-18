# dumpAccessMethod

## Location
src/bin/pg_dump/pg_dump.c: 13274 - 13341

## Overview
Writes out a single access method definition to the pg_dump output, generating both CREATE ACCESS METHOD and DROP ACCESS METHOD statements.

## Definition


## Detailed Description
This function generates SQL statements to recreate an access method during database restoration. It constructs a CREATE ACCESS METHOD statement with the appropriate type (INDEX or TABLE) and handler function, along with a corresponding DROP statement for cleanup. The function handles binary upgrade scenarios and includes support for dumping associated comments.

The function validates the access method type and logs warnings for invalid types. It respects dump options such as data-only mode and component-specific dump flags. The generated statements are registered with the archive system for inclusion in the dump output.

## Parameters / Member Variables
- : Archive handle for output generation and dump options
- : AccessMethodInfo structure containing access method details including name, type, and handler

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer/destroyPQExpBuffer (for SQL statement building)
  - pg_strdup/free (for memory management)
  - fmtId (for proper identifier formatting)
  - appendPQExpBuffer/appendPQExpBufferStr (for statement construction)
  - pg_log_warning (for error logging)
  - binary_upgrade_extension_member (for binary upgrade support)
  - ArchiveEntry (to register dump entry)
  - dumpComment (to handle access method comments)
- Called from (representative examples):
  - dumpDumpableObject (as part of general object dumping)
  - fmtQualifiedDumpable

## Notes and Other Information
- Skips execution in data-only dump mode
- Supports both INDEX and TABLE type access methods
- Validates access method type and handles invalid types gracefully
- Includes binary upgrade support for extension members
- Generates both creation and deletion statements
- Handles access method comments as separate dump components
- Uses proper SQL identifier formatting for access method names
- Part of PostgreSQL's pg_dump utility for database schema export
- Respects component-level dump flags for selective dumping