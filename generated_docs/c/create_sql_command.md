# create_sql_command

## Location
[src/bin/pgbench/pgbench.c:5585-5613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5585-L5613)

## Overview
Creates and initializes a Command structure for SQL commands after filtering out comments and whitespace from the input SQL text.

## Definition

```c
structure */
	my_command = (Command *) pg_malloc(sizeof(Command));
```
## Detailed Description
The create_sql_command function serves as a constructor for SQL Command structures in pgbench. It takes a buffer containing SQL command text and creates a fully initialized Command object ready for execution. The function first uses skip_sql_comments to remove leading whitespace and comments, returning NULL if no executable content remains. For valid SQL content, it allocates memory for a new Command structure and initializes all its fields with appropriate default values.

The function sets up the command type as SQL_COMMAND, initializes statistics tracking, and prepares the command for parameter parsing and execution. This is a critical component in pgbench's command processing pipeline, bridging the gap between raw script parsing and command execution.

## Parameters / Member Variables
- : PQExpBuffer containing the raw SQL command text collected by the parser
- : String identifier for the source of the command (filename or builtin-script ID), used for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [skip_sql_comments](../s/skip_sql_comments.md): Filters out comments and whitespace from SQL text
  - pg_malloc: Allocates memory for the Command structure
  - initPQExpBuffer: Initializes the command's text buffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md): Copies the cleaned SQL text to the command buffer
  - [initSimpleStats](../i/initSimpleStats.md): Initializes statistics tracking for the command
  - SQL_COMMAND: Enum value indicating this is an SQL command type
  - META_NONE: Enum value indicating no special metadata handling
- Called from (representative examples):
  - Script parsing functions that process pgbench input files

## Notes and Other Information
- Returns NULL if the input contains only comments and whitespace, allowing the parser to skip empty commands
- Allocates memory that must be freed later using appropriate cleanup functions
- Initializes all Command structure fields to safe default values
- The first_line field is set to NULL initially and populated later during script processing
- The prepname field is allocated later if prepared statements are used
- Statistics tracking is initialized but will be populated during command execution
- The function assumes the input buffer contains complete, well-formed SQL text from the parser
- Memory allocation uses pg_malloc which will terminate the program on allocation failure