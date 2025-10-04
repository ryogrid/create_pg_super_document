# get_source_line

## Location
[src/pl/plpython/plpy_elog.c:435-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L435-L476)

## Overview
Extracts a specific line from source code text as a palloc'd string, used for error reporting and debugging in PL/Python.

## Definition

```c
static char *
get_source_line(const char *src, int lineno)
```
## Detailed Description
This function parses through source code text to find and extract a specific line number. It iterates through the source text character by character, counting newline characters to track line numbers. Once the target line is found, it skips leading whitespace and returns a copy of the line content. The function handles edge cases such as invalid line numbers, all-whitespace lines, and files that end without a final newline.

The function is primarily used in error reporting contexts where PostgreSQL needs to show the specific line of Python code that caused an error or exception.

## Parameters / Member Variables
- `src`: Pointer to the source code text as a null-terminated string
- `lineno`: The line number to extract (1-based indexing)

## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library function)
  - isspace (standard C library function)  
  - [pstrdup](../p/pstrdup.md) (PostgreSQL memory allocation function)
  - [pnstrdup](../p/pnstrdup.md) (PostgreSQL memory allocation function)
- Called from (representative examples):
  - [PLy_traceback](../P/PLy_traceback.md)

## Notes and Other Information
- Returns NULL for invalid line numbers (≤ 0) or if the requested line doesn't exist
- Automatically strips leading whitespace from the returned line
- Uses PostgreSQL's palloc-based memory management (pstrdup/pnstrdup)
- Handles files that may or may not end with a newline character
- Part of the PL/Python error handling subsystem

## Simplified Source

```c
static char *get_source_line(const char *src, int lineno) {
    const char *s = NULL;
    const char *next = src;
    int current = 0;

    // Validate line number
    if (lineno <= 0)
        return NULL;

    // Find the target line by counting newlines
    while (current < lineno) {
        s = next;
        next = strchr(s + 1, '\n');
        current++;
        if (next == NULL)
            break;
    }

    // Check if we found the requested line
    if (current != lineno)
        return NULL;

    // Skip leading whitespace
    while (*s && isspace((unsigned char) *s))
        s++;

    // Return the line content
    if (next == NULL)
        return pstrdup(s);  // Last line without newline

    // Sanity check for all-whitespace lines
    if (next < s)
        return NULL;

    return pnstrdup(s, next - s);  // Line with newline
}
```