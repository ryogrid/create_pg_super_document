# EndCopy

## Location
src/backend/commands/copyto.c: 314 - 349

## Overview
Releases all resources allocated for a COPY TO operation, including files, pipes, memory contexts, and progress reporting.

## Definition
```c
static void EndCopy(CopyToState cstate)
```

## Detailed Description
EndCopy is the cleanup function for PostgreSQL's COPY TO operations, responsible for properly releasing all resources that were allocated during the copy process. It handles two types of output destinations differently: for program pipes, it calls ClosePipeToProgram() to ensure proper program termination and error handling; for regular files, it uses FreeFile() to close the file handle with appropriate error reporting.

After handling the output destination, the function performs final cleanup by ending progress reporting, deleting the copy-specific memory context (which automatically frees all memory allocated within that context), and freeing the CopyToState structure itself.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing all the state information for the COPY operation that needs to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - ClosePipeToProgram (closes program pipes with error checking)
  - FreeFile (closes regular file handles)
  - ereport (error reporting function)
  - errcode_for_file_access (file access error codes)
  - errmsg (error message formatting)
  - pgstat_progress_end_command (ends progress reporting)
  - MemoryContextDelete (deletes memory context and all allocated memory)
  - pfree (frees the cstate structure)
- Called from (representative examples):
  - DR_copy
  - EndCopyTo

## Notes and Other Information
- This function serves as the primary cleanup routine for COPY TO operations
- Uses different cleanup strategies for program pipes vs regular files
- Automatically handles memory cleanup through memory context deletion
- Progress reporting is properly terminated to avoid resource leaks
- The function is static, indicating it's only used within the copyto.c file
- Essential for proper resource management in PostgreSQL's COPY implementation