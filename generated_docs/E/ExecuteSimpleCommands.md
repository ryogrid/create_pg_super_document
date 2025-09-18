# ExecuteSimpleCommands

## Location
src/bin/pg_dump/pg_backup_db.c: 380 - 444

## Overview
Processes non-COPY table data consisting of multiple INSERT commands by parsing arbitrary buffer boundaries and executing complete SQL statements as they are identified.

## Definition
static void ExecuteSimpleCommands(ArchiveHandle *AH, const char *buf, size_t bufLen)

## Detailed Description
ExecuteSimpleCommands is a sophisticated SQL parser and executor designed specifically for processing INSERT commands and BLOB COMMENTS data during database restoration. The function handles the complex task of parsing SQL commands that may be split across multiple buffer loads, maintaining state information in the ArchiveHandle structure. It implements a simple lexical analyzer that can distinguish between SQL literals, quoted identifiers, and statement-terminating semicolons. The parser uses a finite state machine with three states: SQL_SCAN (default scanning), SQL_IN_SINGLE_QUOTE (inside single-quoted strings), and SQL_IN_DOUBLE_QUOTE (inside double-quoted identifiers). This approach ensures that semicolons within string literals are not mistaken for statement terminators.

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle containing connection info and SQL parsing state
- `buf`: Buffer containing SQL command data to be processed
- `bufLen`: Length of the buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBufferChar
  - [ExecuteSqlCommand](ExecuteSqlCommand.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
- Constants referenced:
  - SQL_SCAN
  - SQL_IN_SINGLE_QUOTE
  - SQL_IN_DOUBLE_QUOTE
- Called from (representative examples):
  - [ExecuteSqlCommandBuf](ExecuteSqlCommandBuf.md)

## Notes and Other Information
- This is a static function limited to pg_backup_db.c scope
- Maintains parsing state across multiple function calls using AH->sqlparse structure
- Handles backslash escaping in single-quoted strings when standard_strings is disabled
- Optimized for INSERT commands but also supports BLOB COMMENTS from pre-9.0 dump files
- Assumes input does not contain SQL comments, E-style literals, or dollar-quoted strings
- Uses lazy initialization for the command buffer (curCmd)
- Strips newlines between commands for cleaner formatting