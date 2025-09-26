# pg_get_line_buf

## Location
[src/common/pg_get_line.c:95-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_get_line.c#L95-L123)

## Overview
A line reading function that collects data from a file stream into a caller-supplied StringInfo buffer, providing a convenient API for processing one line at a time without artificial line length limits.

## Definition
```c
bool pg_get_line_buf(FILE *stream, StringInfo buf)
```

## Detailed Description
`pg_get_line_buf()` provides similar functionality to `pg_get_line()` and `fgets()`, but uses a caller-supplied StringInfo buffer to collect the line data. This design offers a convenient API for code that processes lines sequentially without needing to manage dynamic memory allocation.

The function resets the provided StringInfo buffer before reading, discarding any previous content. It then delegates the actual reading operation to `pg_get_line_append()` with no prompt context for cancellation.

The function returns a boolean indicating success or failure, making error handling straightforward. On failure, the buffer is reset to empty state.

## Parameters / Member Variables
- `stream`: FILE pointer to read from
- `buf`: StringInfo buffer to store the collected line data

## Dependencies
- Functions called/Symbols referenced:
  - resetStringInfo
  - pg_get_line_append

- Called from (representative examples):
  - tsearch_readline (src/backend/tsearch/ts_locale.c:173)
  - readfile (src/bin/initdb/initdb.c:690)
  - read_quoted_string (src/bin/pg_dump/filter.c:241)
  - filter_read_item (src/bin/pg_dump/filter.c:398)
  - SortTocFromFile (src/bin/pg_dump/pg_backup_archiver.c:1565)
  - ecpg_filter_source (src/interfaces/ecpg/test/pg_regress_ecpg.c:55)
  - ecpg_filter_stderr (src/interfaces/ecpg/test/pg_regress_ecpg.c:114)

## Notes and Other Information
- Returns true if a line was successfully collected (including non-newline-terminated lines at EOF)
- Returns false for I/O errors or when no data is available before EOF
- Use `ferror(stream)` to distinguish between I/O errors and EOF conditions
- The buffer is automatically reset to empty on failure cases
- No support for SIGINT-based cancellation (prompt_ctx is always NULL)
- More memory-efficient than `pg_get_line()` for sequential line processing
- Located in src/common/pg_get_line.c:95-123