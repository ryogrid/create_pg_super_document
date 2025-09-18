# dumpTSParser

## Location
src/bin/pg_dump/pg_dump.c: 14587 - 14650

## Overview
Writes out a single text search parser definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH PARSER statement.

## Definition


## Detailed Description
The  function is responsible for dumping text search parser objects during a pg_dump operation. It generates the CREATE TEXT SEARCH PARSER statement with all required function references (START, GETTOKEN, END, optional HEADLINE, and LEXTYPES) and handles proper formatting, binary upgrade compatibility, and comment dumping.

The function skips execution during data-only dumps and constructs both creation and drop statements for the parser. It also handles binary upgrade scenarios and dumps associated comments if requested.

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : TSParserInfo structure containing all parser metadata including function OIDs and parser properties

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - pg_strdup
  - fmtId
  - fmtQualifiedDumpable
  - convertTSFunction
  - appendPQExpBuffer
  - binary_upgrade_extension_member
  - ArchiveEntry
  - dumpComment
  - destroyPQExpBuffer
  - free
- Called from (representative examples):
  - dumpDumpableObject (via switch statement for DO_TSPARSER objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Handles optional HEADLINE function (only included if prsinfo->prsheadline != InvalidOid)
- Supports binary upgrade mode with appropriate extension member handling
- Generates both CREATE and DROP statements for complete dump/restore capability
- Part of PostgreSQL's text search infrastructure dumping functionality
- Uses qualified names to handle schema-qualified parser names properly