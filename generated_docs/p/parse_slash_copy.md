# parse_slash_copy

## Location
[src/bin/psql/copy.c:89-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L89-L267)

## Overview
Parses the arguments of a psql \copy command line and returns a structured representation of the parsed options.

## Definition
```c
static struct copy_options *
parse_slash_copy(const char *args)
```

## Detailed Description
This function parses the complex syntax of psql's \copy command, which supports various forms including table names with optional schema and column lists, query expressions in parentheses, and different file/stream destinations. It handles backward compatibility with the deprecated BINARY keyword, processes schema-qualified table names, column lists in parentheses, FROM/TO direction specification, and various file/program/stream options (STDIN/STDOUT/PSTDIN/PSTDOUT/PROGRAM). The parser uses strtokx for tokenization with proper quote and delimiter handling.

## Parameters / Member Variables
- `args`: String containing the command line arguments for the \copy command

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](pg_malloc0.md), pg_strdup (PostgreSQL memory allocation)
  - [strtokx](../s/strtokx.md) (PostgreSQL tokenizer)
  - [pg_strcasecmp](pg_strcasecmp.md) (PostgreSQL string comparison)
  - [xstrcat](../x/xstrcat.md) (local utility function)
  - [strip_quotes](../s/strip_quotes.md), expand_tilde (path processing)
  - pg_log_error (error reporting)
  - [free_copy_options](../f/free_copy_options.md) (cleanup function)
  - [standard_strings](../s/standard_strings.md)() (PostgreSQL configuration check)
- Called from (representative examples):
  - [do_copy](../d/do_copy.md) (src/bin/psql/copy.c)

## Notes and Other Information
- Supports both legacy 7.3 syntax (with BINARY keyword) and modern syntax
- Handles complex SQL query expressions enclosed in parentheses
- Processes schema-qualified table names (schema.table format)
- Supports column lists in parentheses after table names
- Recognizes special file destinations: STDIN, STDOUT, PSTDIN, PSTDOUT, and PROGRAM commands
- Returns NULL on parsing errors with appropriate error messages logged
- The function builds SQL command components in before_tofrom and after_tofrom fields
- Critical component of psql's \copy command infrastructure