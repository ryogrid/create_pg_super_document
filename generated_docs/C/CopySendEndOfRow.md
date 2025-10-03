# CopySendEndOfRow

## Location
[src/backend/commands/copyto.c:187-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L187-L264)

## Overview
Finalizes and sends a complete row of data in PostgreSQL's COPY operation, handling different output destinations and performing necessary cleanup.

## Definition

```c
static void
CopySendEndOfRow(CopyToState cstate)
```
## Detailed Description
CopySendEndOfRow is a critical function in PostgreSQL's COPY TO implementation that handles the completion of a row's data transmission. It manages three different copy destinations: files, frontend connections, and callback functions. For each destination type, it applies appropriate line termination (platform-specific for files, universal newline for frontend), writes the accumulated row data, and handles any I/O errors that may occur. The function also updates progress statistics and resets the message buffer for the next row.

The function implements sophisticated error handling, particularly for program pipes where it attempts to get more meaningful error messages by closing the pipe and checking the subprocess exit code before falling back to generic pipe errors.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing the current state of the COPY operation, including destination type, options, message buffer, and file handles
## Dependencies
- Functions called/Symbols referenced:
  - [CopySendChar](CopySendChar.md)
  - [CopySendString](CopySendString.md)
  - [ClosePipeToProgram](ClosePipeToProgram.md)
  - pq_putmessage
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [resetStringInfo](../r/resetStringInfo.md)
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - DR_copy
  - [DoCopyTo](../D/DoCopyTo.md)
  - [CopyOneRowTo](CopyOneRowTo.md)

## Notes and Other Information
- Uses platform-specific line endings (\n for Unix, \r\n for Windows) when writing to files in text mode
- Provides special handling for broken pipe errors when writing to program pipes, attempting to get better error diagnostics
- Updates progress reporting statistics after each row is processed
- The function is static, indicating it's only used within the copyto.c file
- Handles both binary and text mode operations appropriately

## Simplified Source

```c
static void
CopySendEndOfRow(CopyToState cstate)
{
    StringInfo fe_msgbuf = cstate->fe_msgbuf;

    switch (cstate->copy_dest)
    {
        case COPY_FILE:
            if (!cstate->opts.binary)
            {
                // Platform-specific line termination
#ifndef WIN32
                CopySendChar(cstate, '\n');
#else
                CopySendString(cstate, "\r\n");
#endif
            }

            if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1, cstate->copy_file) != 1 ||
                ferror(cstate->copy_file))
            {
                if (cstate->is_program)
                {
                    if (errno == EPIPE)
                    {
                        // Try to get better error message from subprocess
                        ClosePipeToProgram(cstate);
                        errno = EPIPE;
                    }
                    ereport(ERROR, "could not write to COPY program");
                }
                else
                    ereport(ERROR, "could not write to COPY file");
            }
            break;

        case COPY_FRONTEND:
            // FE/BE protocol uses \n as newline for all platforms
            if (!cstate->opts.binary)
                CopySendChar(cstate, '\n');

            // Dump the accumulated row as one CopyData message
            (void) pq_putmessage(PqMsg_CopyData, fe_msgbuf->data, fe_msgbuf->len);
            break;

        case COPY_CALLBACK:
            cstate->data_dest_cb(fe_msgbuf->data, fe_msgbuf->len);
            break;
    }

    // Update the progress
    cstate->bytes_processed += fe_msgbuf->len;
    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);

    resetStringInfo(fe_msgbuf);
}
```