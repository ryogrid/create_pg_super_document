# EndCopyFrom

## Location
[src/backend/commands/copyfrom.c:1787-1812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L1787-L1812)

## Overview
Cleans up and releases all resources associated with a COPY FROM operation, including closing files, ending progress reporting, and freeing memory contexts.

## Definition
```c
void
EndCopyFrom(CopyFromState cstate)
```

## Detailed Description
EndCopyFrom performs the final cleanup for a COPY FROM operation by properly releasing all resources that were allocated during BeginCopyFrom and used throughout the COPY process. It handles different types of input sources appropriately - for program-based sources it calls specialized cleanup to properly terminate child processes, while for regular files it closes file handles and reports any errors. The function also terminates progress reporting and deletes the dedicated memory context that was created for the COPY operation, ensuring all associated memory is properly freed.

This function is essential for preventing resource leaks and ensures that external programs are properly terminated, file descriptors are closed, and all memory allocated for the COPY operation is returned to the system.

## Parameters / Member Variables
- `cstate`: CopyFromState structure containing all the state and resources from the COPY FROM operation that need to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ClosePipeFromProgram](../C/ClosePipeFromProgram.md)
  - [FreeFile](../F/FreeFile.md)  
  - [pgstat_progress_end_command](../p/pgstat_progress_end_command.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md) (after COPY FROM completion or error)

## Notes and Other Information
- Must be called to properly clean up after every BeginCopyFrom call, even if the COPY operation fails
- Handles both regular file input and program-based input sources differently
- For program sources, ClosePipeFromProgram ensures child processes are properly terminated
- Reports errors if file closing fails, which could indicate I/O problems or disk issues  
- The progress reporting system is notified that the command has ended
- Memory cleanup is handled by deleting the entire copy context rather than individual deallocations
- This approach ensures complete cleanup even if individual components were not fully initialized
- The cstate structure itself is freed with pfree after the context deletion