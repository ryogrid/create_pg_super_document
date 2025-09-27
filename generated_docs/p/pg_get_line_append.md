# pg_get_line_append

## Location
[src/common/pg_get_line.c:124-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_get_line.c#L124-L180)

## Overview
The core line reading function that appends data from a file stream to an existing StringInfo buffer, supporting backslash continuation patterns and optional SIGINT-based cancellation.

## Definition
```c
bool pg_get_line_append(FILE *stream, StringInfo buf, PromptInterruptContext *prompt_ctx)
```

## Detailed Description
`pg_get_line_append()` is the foundational line reading function that appends collected data to whatever already exists in the provided StringInfo buffer. This append behavior makes it particularly useful for implementing backslash continuation or other line-merging scenarios.

The function employs a reading loop using `fgets()` to incrementally read data into the buffer, automatically expanding the buffer size as needed using `enlargeStringInfo()`. It continues reading until a complete line (terminated by newline) is collected or an error/EOF condition occurs.

The function supports sophisticated SIGINT handling through the `prompt_ctx` parameter, using `sigsetjmp`/`longjmp` mechanisms to enable graceful cancellation during potentially long read operations. When cancellation occurs, the buffer is restored to its original state.

## Parameters / Member Variables
- `stream`: FILE pointer to read from
- `buf`: StringInfo buffer to append the collected line data to
- `prompt_ctx`: Optional context for SIGINT-based cancellation; can be NULL for no cancellation support

## Dependencies
- Functions called/Symbols referenced:
  - sigsetjmp
  - fgets
  - strlen
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - ferror
  - sigjmp_buf (type)
  - [PromptInterruptContext](../P/PromptInterruptContext.md) (struct)

- Called from (representative examples):
  - [tokenize_auth_file](../t/tokenize_auth_file.md) (src/backend/libpq/hba.c:733)
  - [pg_get_line](pg_get_line.md) (src/common/pg_get_line.c:65)
  - [pg_get_line_buf](pg_get_line_buf.md) (src/common/pg_get_line.c:99)

## Notes and Other Information
- Returns true if a line was successfully collected (including non-newline-terminated lines at EOF)
- Returns false for I/O errors, EOF with no data, or cancellation via SIGINT
- Use `ferror(stream)` to distinguish between I/O errors and EOF conditions
- When cancellation occurs, `prompt_ctx->canceled` is set to true
- The buffer contents are preserved on failure (though buffer may be resized)
- The function reads data in chunks, expanding the buffer by 128 bytes when more space is needed
- Serves as the implementation backbone for both `pg_get_line()` and `pg_get_line_buf()`
- Located in src/common/pg_get_line.c:124-180

## Simplified Source

```c
// Simplified version of pg_get_line_append
bool pg_get_line_append(FILE *stream, StringInfo buf, PromptInterruptContext *prompt_ctx) {
    int orig_len = buf->len;

    // Set up cancellation handler if provided
    if (prompt_ctx && sigsetjmp(*((sigjmp_buf *) prompt_ctx->jmpbuf), 1) != 0) {
        // Handle cancellation: restore buffer and return false
        prompt_ctx->canceled = true;
        buf->len = orig_len;
        buf->data[orig_len] = '\0';
        return false;
    }

    // Main reading loop
    for (;;) {
        char *result;

        // Enable cancellation during read
        if (prompt_ctx)
            *(prompt_ctx->enabled) = true;

        // Read data, appending to existing buffer content
        result = fgets(buf->data + buf->len, buf->maxlen - buf->len, stream);

        // Disable cancellation after read
        if (prompt_ctx)
            *(prompt_ctx->enabled) = false;

        // Break if no data read
        if (result == NULL)
            break;

        // Update buffer length with new data
        buf->len += strlen(buf->data + buf->len);

        // Complete line found (ends with newline)
        if (buf->len > orig_len && buf->data[buf->len - 1] == '\n')
            return true;

        // Need more space - expand buffer and continue
        enlargeStringInfo(buf, 128);
    }

    // Handle end of file or error cases
    if (ferror(stream) || buf->len == orig_len) {
        // Restore original buffer state on error
        buf->len = orig_len;
        buf->data[orig_len] = '\0';
        return false;
    }

    // Successfully read data but no newline at EOF
    return true;
}
```

Key simplifications made:
- Added descriptive comments for each major logic block
- Clarified the cancellation mechanism with sigsetjmp/longjmp
- Emphasized the append behavior in comments
- Simplified variable names (res → result)
- Made the buffer expansion logic more explicit
- Highlighted the three possible return scenarios