# CopyFromErrorCallback

## Location
[src/backend/commands/copyfrom.c:112-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L112-L190)

## Overview
CopyFromErrorCallback is an error context callback function for COPY FROM operations that provides detailed error context information including line numbers, column names, and data values when errors occur during data copying.

## Definition

```c
void
CopyFromErrorCallback(void *arg)
```
## Detailed Description
This function serves as the error context callback for COPY FROM operations in PostgreSQL. It takes a CopyFromState argument and generates contextual error messages based on the current state of the copy operation. The function handles different scenarios:

1. **Relation-only context**: When only relation name information is needed
2. **Binary format errors**: For binary COPY operations where data cannot be meaningfully displayed
3. **Text format errors**: For text COPY operations with detailed column and value information

The function intelligently formats error messages to include:
- Relation name
- Current line number
- Column name (if relevant)
- Actual data value (if available and not binary)
- Full line content (when line buffer is valid)

For text format operations, it uses CopyLimitPrintoutLength to ensure error messages don't become excessively long by truncating displayed values when necessary.

## Parameters / Member Variables
- `*arg`: A void pointer that must be cast to CopyFromState, containing the current state of the COPY FROM operation including relation name, line number, column information, and data buffers
## Dependencies
- Functions called/Symbols referenced:
  - [CopyFromState](CopyFromState.md) (struct type)
  - errcontext (error reporting function)
  - CopyLimitPrintoutLength (utility function for limiting output length)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [CopyFrom](CopyFrom.md) (main COPY FROM function at src/backend/commands/copyfrom.c:950)

## Notes and Other Information
- The function is designed to be used with PostgreSQL's error context callback mechanism
- It provides progressively more detailed error information based on what context is available
- Binary format operations have limited error detail display since binary data cannot be meaningfully shown to users
- Memory management is handled properly with pfree() calls for allocated strings
- The function handles NULL values gracefully and provides appropriate messaging
- Line buffer validity is checked before attempting to display line content to avoid displaying stale data

## Simplified Source

```c
void CopyFromErrorCallback(void *arg) {
    CopyFromState cstate = (CopyFromState) arg;

    // Simple relation-only context
    if (cstate->relname_only) {
        errcontext("COPY %s", cstate->cur_relname);
        return;
    }

    if (cstate->opts.binary) {
        // Binary format - can't display data meaningfully
        if (cstate->cur_attname)
            errcontext("COPY %s, line %llu, column %s",
                      cstate->cur_relname,
                      (unsigned long long) cstate->cur_lineno,
                      cstate->cur_attname);
        else
            errcontext("COPY %s, line %llu",
                      cstate->cur_relname,
                      (unsigned long long) cstate->cur_lineno);
    } else {
        // Text format - provide detailed context with data values
        if (cstate->cur_attname && cstate->cur_attval) {
            // Show column-specific error with value
            char *attval = CopyLimitPrintoutLength(cstate->cur_attval);
            errcontext("COPY %s, line %llu, column %s: \"%s\"",
                      cstate->cur_relname,
                      (unsigned long long) cstate->cur_lineno,
                      cstate->cur_attname,
                      attval);
            pfree(attval);
        } else if (cstate->cur_attname) {
            // Column error with NULL value
            errcontext("COPY %s, line %llu, column %s: null input",
                      cstate->cur_relname,
                      (unsigned long long) cstate->cur_lineno,
                      cstate->cur_attname);
        } else {
            // Line-level error
            if (cstate->line_buf_valid) {
                char *lineval = CopyLimitPrintoutLength(cstate->line_buf.data);
                errcontext("COPY %s, line %llu: \"%s\"",
                          cstate->cur_relname,
                          (unsigned long long) cstate->cur_lineno,
                          lineval);
                pfree(lineval);
            } else {
                errcontext("COPY %s, line %llu",
                          cstate->cur_relname,
                          (unsigned long long) cstate->cur_lineno);
            }
        }
    }
}
```