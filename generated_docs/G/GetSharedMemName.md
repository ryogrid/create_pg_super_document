# GetSharedMemName

## Location
[src/backend/port/win32_shmem.c:65-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32_shmem.c#L65-L112)

## Overview
Generates a unique shared memory segment name based on the PostgreSQL data directory path, specifically designed for Windows systems to ensure proper isolation between different PostgreSQL instances.

## Definition


## Detailed Description
This function creates a unique identifier for shared memory segments by expanding the data directory path and converting it into a suitable name format. The function performs several key operations:

1. **Path Expansion**: Uses GetFullPathName() to get the full absolute path of the data directory
2. **Memory Allocation**: Allocates memory for the shared memory name with extra space for the "Global\PostgreSQL:" prefix
3. **Name Construction**: Originally intended to place the segment in the Global\ namespace, but due to permission issues, it overwrites this prefix
4. **Path Normalization**: Converts all backslashes to forward slashes since backslashes aren't permitted in global object names

The function ensures that each PostgreSQL instance with a different data directory gets a unique shared memory segment name, preventing conflicts between multiple postmaster processes.

## Parameters / Member Variables
This function takes no parameters and returns a dynamically allocated string containing the shared memory segment name.

## Dependencies
- Functions called/Symbols referenced:
  - GetFullPathName (Windows API)
  - malloc
  - strcpy  
  - elog
  - GetLastError (Windows API)
- Called from (representative examples):
  - [PGSharedMemoryIsInUse](../P/PGSharedMemoryIsInUse.md)
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md)

## Notes and Other Information
- **Windows-specific**: This function is only compiled and used on Windows platforms (located in win32_shmem.c)
- **Memory Management**: The returned string is dynamically allocated and must be freed by the caller
- **Security Consideration**: Originally designed to use the Global\ namespace for better isolation, but permission issues forced the implementation to use the default namespace
- **Path Handling**: The function handles Windows-specific path formatting and converts backslashes to forward slashes for compatibility
- **Error Handling**: Uses FATAL level logging for critical errors, which will terminate the process if path operations fail