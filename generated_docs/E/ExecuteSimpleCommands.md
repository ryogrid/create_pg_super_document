# ExecuteSimpleCommands

## Location
[src/bin/pg_dump/pg_backup_db.c:380-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L380-L444)

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
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
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

## Simplified Source

```c
static void
ExecuteSimpleCommands(ArchiveHandle *AH, const char *buf, size_t bufLen)
{
    const char *qry = buf;
    const char *eos = buf + bufLen;

    // Initialize command buffer on first use
    if (AH->sqlparse.curCmd == NULL)
        AH->sqlparse.curCmd = createPQExpBuffer();

    // Parse character by character
    for (; qry < eos; qry++) {
        char ch = *qry;

        // Skip newlines between commands for neatness
        if (!(ch == '\n' && AH->sqlparse.curCmd->len == 0))
            appendPQExpBufferChar(AH->sqlparse.curCmd, ch);

        // State machine for SQL parsing
        switch (AH->sqlparse.state) {
            case SQL_SCAN:  // Default scanning state
                if (ch == ';') {
                    // Found statement terminator - execute and reset
                    ExecuteSqlCommand(AH, AH->sqlparse.curCmd->data,
                                    "could not execute query");
                    resetPQExpBuffer(AH->sqlparse.curCmd);
                } else if (ch == '\'') {
                    // Enter single-quoted string
                    AH->sqlparse.state = SQL_IN_SINGLE_QUOTE;
                    AH->sqlparse.backSlash = false;
                } else if (ch == '"') {
                    // Enter double-quoted identifier
                    AH->sqlparse.state = SQL_IN_DOUBLE_QUOTE;
                }
                break;

            case SQL_IN_SINGLE_QUOTE:
                // Handle single-quoted strings with backslash escaping
                if (ch == '\'' && !AH->sqlparse.backSlash)
                    AH->sqlparse.state = SQL_SCAN;
                else if (ch == '\\' && !AH->public.std_strings)
                    AH->sqlparse.backSlash = !AH->sqlparse.backSlash;
                else
                    AH->sqlparse.backSlash = false;
                break;

            case SQL_IN_DOUBLE_QUOTE:
                // Handle double-quoted identifiers
                if (ch == '"')
                    AH->sqlparse.state = SQL_SCAN;
                break;
        }
    }
}
```