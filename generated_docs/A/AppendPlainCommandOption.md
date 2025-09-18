# AppendPlainCommandOption

## Location
src/bin/pg_basebackup/streamutil.c: 812 - 832

## Overview
Appends a "plain" option (option without a value) to a PostgreSQL replication command string, handling both old and new syntax formats with appropriate separators.

## Definition


## Detailed Description
This utility function builds PostgreSQL replication protocol commands by appending option names without values to a command buffer. It handles the syntax differences between old parser keyword style (space-separated) and new parenthesized comma-separated list style. The function intelligently adds appropriate separators (comma and space for new syntax, just space for old syntax) based on the current buffer state and syntax mode being used.

## Parameters / Member Variables
- `buf`: PQExpBuffer containing the command being constructed
- `use_new_option_syntax`: Boolean flag indicating which syntax format to use (true for new comma-separated syntax, false for old space-separated syntax)
- `option_name`: Name of the option to append (without a value)

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) - Append string to buffer
  - appendPQExpBufferChar - Append single character to buffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) - Append formatted string to buffer
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup.c:1895, 1903, 1905, 1912, 1919, 1925, 1932, 1955)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (streamutil.c:685, 691, 701)
  - [AppendStringCommandOption](AppendStringCommandOption.md) (streamutil.c:836)
  - [AppendIntegerCommandOption](AppendIntegerCommandOption.md) (streamutil.c:859)

## Notes and Other Information
- Handles two PostgreSQL command syntax formats:
  - Old syntax: COMMAND OPTION1 OPTION2 'value' OPTION3 42
  - New syntax: COMMAND (OPTION1, OPTION2 'value', OPTION3 42)
- Automatically detects if separator is needed by checking buffer state (avoids separator after opening parenthesis)
- Used for building replication protocol commands like BASE_BACKUP and CREATE_REPLICATION_SLOT
- Part of a family of command option appending functions (along with AppendStringCommandOption and AppendIntegerCommandOption)
- Always adds a space before the option name regardless of syntax mode
- In new syntax mode, adds ", " separator before the space and option name when needed
- In old syntax mode, adds just " " separator when needed