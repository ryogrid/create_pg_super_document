# handleCopyIn

## Location
[src/bin/psql/copy.c:511-727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L511-L727)

## Overview
Handles the client-side input phase of a COPY FROM STDIN command in psql, reading data from a file stream and sending it to the PostgreSQL server.

## Definition

```c
bool
handleCopyIn(PGconn *conn, FILE *copystream, bool isbinary, PGresult **res)
```
## Detailed Description
The handleCopyIn function manages the data input phase of PostgreSQL's COPY FROM STDIN operation from the psql client side. It reads data from a specified file stream (which can be stdin or a regular file) and transmits it to the server using libpq's copy protocol functions. The function handles both binary and text modes, with text mode requiring special processing to detect the EOF marker (\.) that terminates the copy operation.

For text mode, the function reads data line by line to properly detect the EOF marker without consuming data beyond it (important when COPY commands are embedded in SQL scripts). For binary mode, it reads data in chunks directly. The function also handles user interrupts gracefully using longjmp/setjmp and provides interactive prompts when reading from a terminal.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle
- `*copystream`: File stream to read data from (stdin, regular file, etc.)
- `isbinary`: Boolean flag indicating whether to use binary or text copy mode
- `**res`: Pointer to store the final PGresult from the copy operation
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

## Simplified Source

```c
bool handleCopyIn(PGconn *conn, FILE *copystream, bool isbinary, PGresult **res) {
    bool success = true;
    char buffer[COPYBUFSIZ];
    bool show_prompt;

    // Set up interrupt handling for user cancellation
    if (sigsetjmp(sigint_interrupt_jmp, 1) != 0) {
        // User interrupted - terminate copy operation
        PQputCopyEnd(conn, "canceled by user");
        success = false;
        goto cleanup;
    }

    // Show prompts if reading from terminal
    show_prompt = isatty(fileno(copystream));
    if (show_prompt && !pset.quiet) {
        puts("Enter data to be copied followed by a newline.\n"
             "End with a backslash and a period on a line by itself, or an EOF signal.");
    }

    if (isbinary) {
        // Binary mode: read data in chunks
        for (;;) {
            sigint_interrupt_enabled = true;
            int bytes_read = fread(buffer, 1, COPYBUFSIZ, copystream);
            sigint_interrupt_enabled = false;

            if (bytes_read <= 0)
                break;

            if (PQputCopyData(conn, buffer, bytes_read) <= 0) {
                success = false;
                break;
            }
        }
    } else {
        // Text mode: read line by line to detect EOF marker (\.)
        bool copy_done = false;
        int buffer_len = 0;
        bool at_line_start = true;

        while (!copy_done) {
            if (at_line_start && show_prompt) {
                fputs(get_prompt(PROMPT_COPY, NULL), stdout);
                fflush(stdout);
            }

            sigint_interrupt_enabled = true;
            char *line_result = fgets(&buffer[buffer_len],
                                      COPYBUFSIZ - buffer_len, copystream);
            sigint_interrupt_enabled = false;

            if (!line_result) {
                copy_done = true;
            } else {
                int line_length = strlen(line_result);
                buffer_len += line_length;

                // Check for complete line and EOF marker
                if (buffer[buffer_len - 1] == '\n') {
                    if (at_line_start &&
                        (line_length == 3 && memcmp(line_result, "\\.\n", 3) == 0)) {
                        copy_done = true;
                    }
                    at_line_start = true;
                } else {
                    at_line_start = false;
                }
            }

            // Send buffer when full or copy complete
            if (buffer_len >= COPYBUFSIZ - 5 || (copy_done && buffer_len > 0)) {
                if (PQputCopyData(conn, buffer, buffer_len) <= 0) {
                    success = false;
                    break;
                }
                buffer_len = 0;
            }
        }
    }

    // Check for read errors
    if (ferror(copystream))
        success = false;

    // Terminate the copy operation
    PQputCopyEnd(conn, success ? NULL : "aborted because of read failure");

cleanup:
    clearerr(copystream);

    // Ensure we exit COPY_IN state
    while (*res = PQgetResult(conn), PQresultStatus(*res) == PGRES_COPY_IN) {
        success = false;
        PQclear(*res);
        PQputCopyEnd(conn, "trying to exit copy mode");
    }

    if (PQresultStatus(*res) != PGRES_COMMAND_OK) {
        pg_log_info("%s", PQerrorMessage(conn));
        success = false;
    }

    return success;
}
```