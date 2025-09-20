# OT_WHOLE_LINE

## Location
[src/bin/psql/psqlscanslash.h:21-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/psqlscanslash.h#L21-L40)

## Overview
OT_WHOLE_LINE is an enumeration constant in psql that instructs the slash command option parser to consume and return the entire remainder of the current input line as a single parameter.

## Definition

```c
enum slash_option_type type,
					   char *quote,
					   bool semicolon);
```
## Detailed Description
OT_WHOLE_LINE is one of the values in the  enumeration used by psql's slash command parsing system. When this option type is specified to , the parser will consume everything from the current position to the end of the line and return it as a single string parameter, regardless of spaces, quotes, or other delimiters that would normally separate arguments.

This parsing mode is particularly useful for slash commands that need to process arbitrary text content that may contain spaces and special characters, such as shell commands, SQL statements, or file paths with embedded spaces.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter to 
- Called from (representative examples):
  -  in src/bin/psql/command.c:281
  -  in src/bin/psql/command.c:722
  -  in src/bin/psql/command.c:1185
  -  in src/bin/psql/command.c:1668
  -  in src/bin/psql/command.c:2537
  -  in src/bin/psql/command.c:3057
  -  in src/bin/psql/command.c:3246

## Notes and Other Information
- Used primarily for commands that need to process the entire remainder of a line as a single argument
- Common use cases include shell escape commands (\!) where the entire command line should be passed to the shell
- Also used for commands like \copy where complex syntax may include spaces and special characters
- The  function uses this option type to discard unwanted trailing content from slash command lines
- This parsing mode bypasses normal tokenization rules and quote processing