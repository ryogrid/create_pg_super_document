# dumpTSDictionary

## Location
src/bin/pg_dump/pg_dump.c: 14651 - 14730

## Overview
Writes out a single text search dictionary definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH DICTIONARY statement with template and initialization options.

## Definition


## Detailed Description
The  function is responsible for dumping text search dictionary objects during a pg_dump operation. It generates the CREATE TEXT SEARCH DICTIONARY statement by fetching the dictionary's template information from the database and including any initialization options. The function constructs both creation and drop statements, handles binary upgrade scenarios, and dumps associated comments and ownership information.

The function performs a database query to retrieve the template namespace and name from pg_ts_template and pg_namespace system catalogs to properly reference the dictionary's template in the dump output.

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : TSDictInfo structure containing dictionary metadata including template OID, initialization options, and ownership information

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - destroyPQExpBuffer
  - pg_strdup
  - fmtId
  - fmtQualifiedDumpable
  - appendPQExpBuffer
  - appendPQExpBufferStr
  - ExecuteSqlQueryForSingleRow
  - PQgetvalue
  - PQclear
  - binary_upgrade_extension_member
  - ArchiveEntry
  - dumpComment
  - free
- Called from (representative examples):
  - dumpDumpableObject (via switch statement for DO_TSDICT objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Queries the database to resolve template references to fully qualified template names
- Handles optional initialization parameters (dictinitoption) when present
- Supports binary upgrade mode with appropriate extension member handling
- Includes owner information in the archive entry for proper ownership restoration
- Generates both CREATE and DROP statements for complete dump/restore capability
- Part of PostgreSQL's text search infrastructure dumping functionality
- Uses qualified names to handle schema-qualified dictionary and template names properly