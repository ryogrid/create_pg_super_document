# RestoreLibraryState

## Location
[src/backend/utils/fmgr/dfmgr.c:676-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L676-L683)

## Overview
Loads all dynamic libraries from a serialized state buffer to restore the same library environment in a parallel worker process.

## Definition
```c
void RestoreLibraryState(char *start_address)
```

## Detailed Description
This function reads a serialized library state buffer (created by SerializeLibraryState) and loads each library listed in the buffer. It iterates through null-terminated filenames in the buffer, calling internal_load_library() for each one until it encounters the terminating null byte. This allows parallel worker processes to recreate the exact same set of loaded dynamic libraries as the leader process, ensuring function pointers and library symbols are available for parallel execution.

## Parameters / Member Variables
- `start_address`: Pointer to the beginning of the serialized library state buffer containing null-terminated filenames

## Dependencies
- Functions called/Symbols referenced:
  - [internal_load_library](../i/internal_load_library.md) (loads a single dynamic library by filename)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)
  - OidFunctionCall9

## Notes and Other Information
- Expects the buffer format created by SerializeLibraryState (null-terminated filenames followed by empty string)
- Uses strlen() to advance through the buffer after processing each filename
- Terminates when it encounters a null byte (empty string) in the buffer
- Part of the parallel query infrastructure completing the trio with EstimateLibraryStateSpace and SerializeLibraryState
- Critical for ensuring parallel workers have access to the same extension functions as the leader process
- Called during parallel worker initialization to establish the proper library environment

## Simplified Source

```c
void RestoreLibraryState(char *start_address)
{
    /* Load each library from the serialized buffer */
    while (*start_address != '\0') {
        internal_load_library(start_address);
        start_address += strlen(start_address) + 1;
    }
}
```