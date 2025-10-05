# lookahead

## Location
[src/tools/pg_bsd_indent/io.c:275-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L275-L319)

## Overview
Provides look-ahead capability for reading input characters beyond the current buffer position without consuming them permanently.

## Definition

```c
int
lookahead(void)
```
## Detailed Description
The lookahead function enables reading characters from the input stream ahead of the current position in the main input buffer. It maintains a separate lookahead buffer that can be read multiple times and reset as needed. This is essential for parsing decisions that require examining upcoming tokens without committing to consuming them.

The function first checks if there are saved characters from a previous buffer state (bp_save area), then manages a dynamically-sized lookahead buffer. When the lookahead buffer is exhausted, it reads additional characters from the input file, automatically expanding the buffer as needed. The function filters out null characters to maintain consistency with the main buffer filling logic.

## Parameters / Member Variables
This function takes no parameters but uses several global variables:
- : Pointer to saved buffer characters for restoration
- : End pointer for the saved buffer area
- : Current position in the lookahead buffer
- : End of valid data in the lookahead buffer  
- : Start of the dynamically allocated lookahead buffer
- : End of the allocated lookahead buffer space
- : Input file stream for reading new characters

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for initial lookahead buffer allocation)
  - realloc (for expanding the lookahead buffer when needed)
  - [errx](../e/errx.md) (for fatal error reporting on allocation failure)
- Called from (representative examples):
  - [is_func_definition](../i/is_func_definition.md) (in lexi.c for parsing function definitions)
  - [_discoverArchiveFormat](../d/_discoverArchiveFormat.md) (in pg_backup_archiver.c for archive format detection)
  - [_tarReadRaw](../t/_tarReadRaw.md) (in pg_backup_tar.c for tar format processing)

## Notes and Other Information
- Returns the next character as an unsigned char cast to int, or EOF when end of input is reached
- Automatically manages buffer allocation, starting with 64 bytes and doubling as needed
- Null characters are skipped to maintain consistency with main buffer behavior
- Must be paired with lookahead_reset() calls to avoid losing synchronization with the main buffer
- Critical for multi-character lookahead in parsing contexts where token recognition requires examining future characters
- Used extensively in pg_bsd_indent for parsing decisions and in pg_dump utilities for format detection

## Simplified Source

```c
int
lookahead(void)
{
    // First check saved buffer area
    if (lookahead_bp_save != NULL && lookahead_bp_save < be_save)
        return (unsigned char) *lookahead_bp_save++;

    // Read from main lookahead buffer, expanding as needed
    while (lookahead_ptr >= lookahead_end) {
        int ch = getc(input);

        if (ch == EOF)
            return ch;
        if (ch == '\0')
            continue;  // Skip nulls like fill_buffer does

        // Expand buffer if full
        if (lookahead_end >= lookahead_buf_end) {
            char *new_buf;
            size_t req = lookahead_buf ? (lookahead_buf_end - lookahead_buf) * 2 : 64;

            new_buf = lookahead_buf ? realloc(lookahead_buf, req) : malloc(req);
            if (new_buf == NULL)
                errx(1, "too much lookahead required");

            // Update all pointers for new buffer location
            lookahead_start = new_buf + (lookahead_start - lookahead_buf);
            lookahead_ptr = new_buf + (lookahead_ptr - lookahead_buf);
            lookahead_end = new_buf + (lookahead_end - lookahead_buf);
            lookahead_buf = new_buf;
            lookahead_buf_end = new_buf + req;
        }

        *lookahead_end++ = ch;
    }

    return (unsigned char) *lookahead_ptr++;
}
```