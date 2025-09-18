# pgwin32_open_handle

## Location
src/port/open.c: 65 - 157

## Overview
Creates a Windows file handle with PostgreSQL-specific error handling and retry logic, providing the core file opening functionality for the Windows port.

## Definition
```c
HANDLE pgwin32_open_handle(const char *fileName, int fileFlags, bool backup_semantics)
```

## Detailed Description
This function serves as the internal workhorse for PostgreSQL's Windows file operations. It translates POSIX-style file flags to Windows CreateFile() parameters and implements sophisticated error handling:

1. **Flag Translation**: Converts POSIX flags (O_RDONLY, O_WRONLY, O_RDWR, O_APPEND, etc.) to Windows access rights and attributes
2. **Retry Logic**: Implements a retry mechanism for sharing violations and lock violations, retrying for up to 30 seconds (300 attempts with 100ms delays)
3. **Special Error Handling**: Handles Windows-specific scenarios like STATUS_DELETE_PENDING (files marked for deletion but not yet removed)
4. **Security**: Sets up inheritable handles with appropriate sharing permissions
5. **Performance Hints**: Supports Windows-specific flags like FILE_FLAG_RANDOM_ACCESS and FILE_FLAG_SEQUENTIAL_SCAN

The function can optionally enable backup semantics to allow opening directories for limited operations.

## Parameters / Member Variables
- `fileName`: Path to the file to open
- `fileFlags`: POSIX-style file flags (O_RDONLY, O_WRONLY, O_CREAT, etc.)
- `backup_semantics`: When true, enables FILE_FLAG_BACKUP_SEMANTICS to allow directory access

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_ntdll](../i/initialize_ntdll.md)
  - [openFlagsToCreateFileFlags](../o/openFlagsToCreateFileFlags.md)
  - [pg_usleep](pg_usleep.md)
  - _dosmaperr
  - pg_RtlGetLastNtStatus
- Called from (representative examples):
  - [pgwin32_open](pgwin32_open.md)
  - [_pglstat64](_pglstat64.md)

## Notes and Other Information
- Returns INVALID_HANDLE_VALUE on failure with errno set appropriately
- Includes extensive assertion checking to validate supported flag combinations
- Implements antivirus/backup software tolerance through the retry mechanism
- Handles Windows-specific file sharing semantics that allow concurrent rename/unlink operations
- The backup_semantics parameter is primarily used by stat() operations that need to access directory metadata
- Contains conditional compilation for FRONTEND vs backend error reporting