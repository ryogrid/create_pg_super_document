# pg_get_line

## Location
[src/common/pg_get_line.c:59-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_get_line.c#L59-L94)

## Overview
A dynamic string reading function that reads a line from a file stream into a palloc'd string buffer, automatically resizing the buffer to handle arbitrarily long input lines.

## Definition
```c
char *pg_get_line(FILE *stream, PromptInterruptContext *prompt_ctx)
```

## Detailed Description
`pg_get_line()` is designed as an equivalent to the standard `fgets()` function, but with significant improvements for handling variable-length input lines. Instead of reading into a fixed-size caller-supplied buffer, it dynamically allocates and resizes a palloc'd (or malloc'd in frontend) string buffer as needed to accommodate indefinitely long input lines.

The function preserves the trailing newline character (if present) in the returned string, maintaining compatibility with `fgets()` behavior. Callers may apply `pg_strip_crlf()` if newline removal is desired.

The function supports optional cancellation via SIGINT signal handling through the `prompt_ctx` parameter, allowing for graceful interruption of long-running read operations.

## Parameters / Member Variables
- `stream`: FILE pointer to read from
- `prompt_ctx`: Optional context for SIGINT-based cancellation; can be NULL for no cancellation support

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [pg_get_line_append](pg_get_line_append.md)
  - [pfree](pfree.md)
  - [PromptInterruptContext](../P/PromptInterruptContext.md) (struct)

- Called from (representative examples):
  - [get_su_pwd](../g/get_su_pwd.md) (src/bin/initdb/initdb.c:1676)
  - [pipe_read_line](pipe_read_line.md) (src/common/exec.c:388)
  - [simple_prompt_extended](../s/simple_prompt_extended.md) (src/common/sprompt.c:145)

## Notes and Other Information
- The function returns NULL on I/O error or EOF with no data, with errno preserved for error distinction
- Memory allocation errors will trigger ereport(ERROR) or exit(1) rather than being returned to caller
- The allocated buffer is typically larger than strictly necessary; for memory-conscious applications collecting many long-lived strings, consider using `pg_get_line_buf()` or `pg_get_line_append()` in a loop with `pstrdup()`
- The caller is responsible for pfree'ing the returned string
- Located in src/common/pg_get_line.c:59-94