# pgwin32_open_handle

## Location
[src/port/open.c:65-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/open.c#L65-L157)

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
  - [_dosmaperr](../d/_dosmaperr.md)
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

## Simplified Source

```c
HANDLE
pgwin32_open_handle(const char *fileName, int fileFlags, bool backup_semantics)
{
    HANDLE h;
    SECURITY_ATTRIBUTES sa;
    int loops = 0;

    // Initialize NT DLL access
    if (initialize_ntdll() < 0)
        return INVALID_HANDLE_VALUE;

    // Validate supported file flags
    assert((fileFlags & ((O_RDONLY | O_WRONLY | O_RDWR) | O_APPEND |
                         (O_RANDOM | O_SEQUENTIAL | O_TEMPORARY) |
                         _O_SHORT_LIVED | O_DSYNC | O_DIRECT |
                         (O_CREAT | O_TRUNC | O_EXCL) | (O_TEXT | O_BINARY))) == fileFlags);

    // Set up security attributes for inheritable handles
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    sa.lpSecurityDescriptor = NULL;

    // Retry loop for sharing violations
    while ((h = CreateFile(fileName,
                           // Translate access mode flags
                           (fileFlags & O_RDWR) ? (GENERIC_WRITE | GENERIC_READ) :
                           ((fileFlags & O_WRONLY) ? GENERIC_WRITE : GENERIC_READ),
                           // Allow concurrent operations
                           (FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE),
                           &sa,
                           openFlagsToCreateFileFlags(fileFlags),
                           // Build file attributes and flags
                           FILE_ATTRIBUTE_NORMAL |
                           (backup_semantics ? FILE_FLAG_BACKUP_SEMANTICS : 0) |
                           ((fileFlags & O_RANDOM) ? FILE_FLAG_RANDOM_ACCESS : 0) |
                           ((fileFlags & O_SEQUENTIAL) ? FILE_FLAG_SEQUENTIAL_SCAN : 0) |
                           ((fileFlags & _O_SHORT_LIVED) ? FILE_ATTRIBUTE_TEMPORARY : 0) |
                           ((fileFlags & O_TEMPORARY) ? FILE_FLAG_DELETE_ON_CLOSE : 0) |
                           ((fileFlags & O_DIRECT) ? FILE_FLAG_NO_BUFFERING : 0) |
                           ((fileFlags & O_DSYNC) ? FILE_FLAG_WRITE_THROUGH : 0),
                           NULL)) == INVALID_HANDLE_VALUE)
    {
        DWORD err = GetLastError();

        // Handle sharing and lock violations (antivirus, backup software)
        if (err == ERROR_SHARING_VIOLATION || err == ERROR_LOCK_VIOLATION)
        {
#ifndef FRONTEND
            // Log warning after 5 seconds of retrying
            if (loops == 50)
                ereport(LOG,
                        (errmsg("could not open file \"%s\": %s", fileName,
                               (err == ERROR_SHARING_VIOLATION) ? _("sharing violation") : _("lock violation")),
                         errdetail("Continuing to retry for 30 seconds."),
                         errhint("You might have antivirus, backup, or similar software interfering with the database system.")));
#endif

            // Retry for up to 30 seconds
            if (loops < 300)
            {
                pg_usleep(100000);  // Wait 100ms
                loops++;
                continue;
            }
        }

        // Handle STATUS_DELETE_PENDING (file marked for deletion)
        if (err == ERROR_ACCESS_DENIED &&
            pg_RtlGetLastNtStatus() == STATUS_DELETE_PENDING)
        {
            // If creating, report file exists; if opening, report not found
            if (fileFlags & O_CREAT)
                err = ERROR_FILE_EXISTS;
            else
                err = ERROR_FILE_NOT_FOUND;
        }

        // Map Windows error to errno and return failure
        _dosmaperr(err);
        return INVALID_HANDLE_VALUE;
    }

    return h;  // Success
}
```