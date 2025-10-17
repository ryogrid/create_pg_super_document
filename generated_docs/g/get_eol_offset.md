# get_eol_offset

## Location
[src/bin/pg_combinebackup/backup_label.c:201-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/backup_label.c#L201-L223)

## Overview
A static utility function that finds the offset position of the next line in a StringInfo buffer or returns the buffer end position if no newline is found.

## Definition

```c
static int
get_eol_offset(StringInfo buf)
```
## Detailed Description
The  function scans through a StringInfo buffer starting from the current cursor position to find the next newline character ('\n'). When a newline is found, it returns the offset position immediately after the newline character (the start of the next line). If no newline character is found before reaching the end of the buffer, it returns the buffer's end position.

This function is essential for line-by-line processing of backup label files, allowing parser functions to safely identify line boundaries without going beyond the buffer limits. The function preserves the buffer's cursor position and only returns offset information without modifying the buffer state.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing text data to scan for line endings
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic buffer operations)
- Called from (representative examples):
  - [parse_backup_label](../p/parse_backup_label.md)
  - [write_backup_label](../w/write_backup_label.md)

## Notes and Other Information
- Static function scope limits visibility to the backup_label.c source file
- Returns position after newline character, not the position of the newline itself
- Safe for buffers without trailing newlines - returns buffer end position
- Cursor position in the buffer is preserved and used as the starting search position
- Used extensively in backup label parsing to process files line by line
- Handles both Unix-style line endings (\n) - Windows-style line endings (\r\n) would require additional logic

## Simplified Source

```c
static int
get_eol_offset(StringInfo buf)
{
    int offset = buf->cursor;

    // Search for newline character starting from cursor
    while (offset < buf->len)
    {
        if (buf->data[offset] == '\n')
            return offset + 1;  // Return position after newline
        ++offset;
    }

    // No newline found, return end of buffer
    return offset;
}
```