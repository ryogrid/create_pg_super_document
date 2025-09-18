# CopySendEndOfRow

## Location
src/backend/commands/copyto.c: 187 - 264

## Overview
Finalizes and sends a complete row of data in PostgreSQL's COPY operation, handling different output destinations and performing necessary cleanup.

## Definition


## Detailed Description
CopySendEndOfRow is a critical function in PostgreSQL's COPY TO implementation that handles the completion of a row's data transmission. It manages three different copy destinations: files, frontend connections, and callback functions. For each destination type, it applies appropriate line termination (platform-specific for files, universal newline for frontend), writes the accumulated row data, and handles any I/O errors that may occur. The function also updates progress statistics and resets the message buffer for the next row.

The function implements sophisticated error handling, particularly for program pipes where it attempts to get more meaningful error messages by closing the pipe and checking the subprocess exit code before falling back to generic pipe errors.

## Parameters / Member Variables
- : CopyToState structure containing the current state of the COPY operation, including destination type, options, message buffer, and file handles

## Dependencies
- Functions called/Symbols referenced:
  - CopySendChar
  - CopySendString
  - ClosePipeToProgram
  - pq_putmessage
  - pgstat_progress_update_param
  - resetStringInfo
  - ereport
  - errcode_for_file_access
  - errmsg
- Called from (representative examples):
  - DR_copy
  - DoCopyTo
  - CopyOneRowTo

## Notes and Other Information
- Uses platform-specific line endings (\n for Unix, \r\n for Windows) when writing to files in text mode
- Provides special handling for broken pipe errors when writing to program pipes, attempting to get better error diagnostics
- Updates progress reporting statistics after each row is processed
- The function is static, indicating it's only used within the copyto.c file
- Handles both binary and text mode operations appropriately