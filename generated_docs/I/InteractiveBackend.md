# InteractiveBackend

## Location
src/backend/tcop/postgres.c: 248 - 335

## Overview
A function that handles interactive user input in PostgreSQL's backend process, reading SQL commands from stdin and preparing them for execution.

## Definition


## Detailed Description
The `InteractiveBackend` function is responsible for handling interactive user connections in PostgreSQL's backend. It displays a prompt to the user ("backend> "), reads input from stdin character by character, and processes the input to form complete SQL commands. The function supports two different input modes:

1. **Plain mode**: Commands are terminated by newlines (unless escaped with backslash)
2. **Semicolon-newline-newline mode** (-j mode): Commands are terminated by a semicolon followed by two newlines

The function handles line continuation with backslashes in plain mode and properly manages the input buffer. Once a complete command is assembled, it returns `PqMsg_Query` to indicate that a query message is ready for processing. If EOF is encountered with no input, it returns EOF to signal shutdown.

## Parameters / Member Variables
- `inBuf`: A StringInfo buffer where the assembled user input (SQL command) is stored. The function resets this buffer at the start and builds the command character by character.

## Dependencies
- Functions called/Symbols referenced:
  - printf() (standard C library)
  - fflush() (standard C library)
  - resetStringInfo() (PostgreSQL string utility)
  - [interactive_getc](../i/interactive_getc.md)() (custom input function)
  - appendStringInfoChar() (PostgreSQL string utility)
  - UseSemiNewlineNewline (global variable)
  - EchoQuery (global variable)
  - PqMsg_Query (message type constant)

- Called from (representative examples):
  - [ReadCommand](../R/ReadCommand.md) (src/backend/tcop/postgres.c:499)

## Notes and Other Information
- This function is static, meaning it's only accessible within the postgres.c compilation unit
- The function supports different input modes controlled by the `UseSemiNewlineNewline` global variable
- Line continuation is supported using backslash escaping in plain mode
- The function echoes the assembled query if the `EchoQuery` flag is set
- The assembled command is null-terminated before returning to make it compatible with message processing
- This function is primarily used for debugging and development purposes when PostgreSQL is run in interactive mode
- EOF handling allows for graceful shutdown when the input stream is closed