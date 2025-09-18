# handleCopyIn

## Location
src/bin/psql/copy.c: 511 - 727

## Overview
Handles the client-side input phase of a COPY FROM STDIN command in psql, reading data from a file stream and sending it to the PostgreSQL server.

## Definition


## Detailed Description
The handleCopyIn function manages the data input phase of PostgreSQL's COPY FROM STDIN operation from the psql client side. It reads data from a specified file stream (which can be stdin or a regular file) and transmits it to the server using libpq's copy protocol functions. The function handles both binary and text modes, with text mode requiring special processing to detect the EOF marker (\.) that terminates the copy operation.

For text mode, the function reads data line by line to properly detect the EOF marker without consuming data beyond it (important when COPY commands are embedded in SQL scripts). For binary mode, it reads data in chunks directly. The function also handles user interrupts gracefully using longjmp/setjmp and provides interactive prompts when reading from a terminal.

## Parameters / Member Variables
- : PostgreSQL database connection handle
- : File stream to read data from (stdin, regular file, etc.)
- : Boolean flag indicating whether to use binary or text copy mode
- : Pointer to store the final PGresult from the copy operation

## Dependencies
- Functions called/Symbols referenced:
  - sigsetjmp (signal handling for interrupts)
  - [PQputCopyEnd](../P/PQputCopyEnd.md) (ends the copy operation)
  - [PQprotocolVersion](../P/PQprotocolVersion.md) (checks PostgreSQL protocol version)
  - get_prompt (gets appropriate prompt string)
  - [PQputCopyData](../P/PQputCopyData.md) (sends copy data to server)
  - [PQgetResult](../P/PQgetResult.md) (gets result from server)
  - pg_log_info (logs error messages)
  - PROMPT_COPY (prompt type constant)
  - COPYBUFSIZ (buffer size constant)
  - PGRES_COPY_IN, PGRES_COMMAND_OK (result status constants)
- Called from (representative examples):
  - [HandleCopyResult](../H/HandleCopyResult.md) (src/bin/psql/common.c:936)

## Notes and Other Information
- The function includes special handling for interactive terminals, showing prompts and usage instructions
- Text mode processing has a known limitation where '\.' inside quoted CSV strings incorrectly terminates the copy operation
- Uses longjmp for graceful handling of user interrupts (Ctrl+C)
- Maintains line number tracking when reading from the current command source
- Ensures proper cleanup of connection state even if copy operation fails
- Buffer management ensures EOF markers are never split across read operations
- Compatible with both protocol version 2 and 3, though version 2 support is maintained for backward compatibility