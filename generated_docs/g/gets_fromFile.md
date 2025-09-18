# gets_fromFile

## Location
src/bin/psql/input.c: 186 - 276

## Overview
Reads a complete line of non-interactive input from a file (including stdin), handling multi-chunk reads and proper memory management with SIGINT safety.

## Definition
```c
char *gets_fromFile(FILE *source)
```

## Detailed Description
This function provides a robust mechanism for reading complete lines from file input sources, designed to handle cases where a single line might be longer than the internal buffer size. It uses a static PQExpBuffer that persists across calls to avoid memory leaks when interrupted by SIGINT signals.

The function operates in a loop, reading chunks of data using fgets() and accumulating them in the buffer until a complete line (terminated by newline) is read. It properly handles EOF conditions, file errors, and memory allocation failures. The function is designed to be SIGINT-safe by enabling/disabling interrupt handling around the critical fgets() call.

Key features:
- Uses a static buffer that persists across calls for memory leak prevention
- Handles arbitrarily long lines by reading in chunks
- Proper SIGINT signal handling during file operations
- Comprehensive error handling for file errors and memory issues
- Returns malloc'd strings that must be freed by the caller

## Parameters / Member Variables
- `source`: FILE pointer to read from (can be any file, including stdin)

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - fgets
  - ferror
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - PQExpBufferBroken
  - pg_log_error
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [gets_interactive](gets_interactive.md)
  - [MainLoop](../M/MainLoop.md)
  - [exec_command_prompt](../e/exec_command_prompt.md)

## Notes and Other Information
- Caller must have set up sigint_interrupt_jmp before calling
- Returns a malloc'd string that must be freed by the caller
- Returns NULL on EOF or input error
- Uses a static PQExpBuffer to prevent memory leaks during SIGINT interruption
- Handles lines longer than the internal buffer size (1024 bytes) by reading in chunks
- Strips the trailing newline from the returned string
- Thread safety: uses static buffer, so not thread-safe
- The function can handle binary data but treats newline as line terminator