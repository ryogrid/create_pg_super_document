# crashDumpHandler

## Location
[src/backend/port/win32/crashdump.c:90-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/crashdump.c#L90-L177)

## Overview
A Windows-specific exception handler that generates crash dump files when PostgreSQL encounters unhandled exceptions, providing debugging information for post-mortem analysis.

## Definition

```c
static LONG WINAPI
crashDumpHandler(struct _EXCEPTION_POINTERS *pExceptionInfo)
```
## Detailed Description
This function serves as PostgreSQL's unhandled exception filter on Windows systems. When an unhandled exception occurs, this handler attempts to generate a minidump file containing process state information for debugging purposes. The function operates in a crash context, so it avoids using PostgreSQL functions and works directly with Windows APIs.

The handler checks for the existence of a "crashdumps" directory in the PostgreSQL data directory before attempting to create dump files. It uses the Windows Debug Help Library (dbghelp.dll) to generate comprehensive minidumps that include process memory, handle data, and data segments while excluding shared memory and memory-mapped files.

The function generates uniquely named dump files using the process ID and system tick count to prevent filename collisions. After attempting to write the dump, it returns EXCEPTION_CONTINUE_SEARCH to allow Windows to continue with its normal exception handling process.

## Parameters / Member Variables
- `*pExceptionInfo`: Pointer to EXCEPTION_POINTERS structure containing detailed information about the exception that occurred, including the exception record and processor context
## Dependencies
- Functions called/Symbols referenced:
  - GetFileAttributesA (Windows API)
  - LoadLibrary (Windows API) 
  - GetProcAddress (Windows API)
  - MiniDumpWriteDump (dbghelp.dll)
  - GetCurrentProcess (Windows API)
  - GetProcessId (Windows API)
  - GetCurrentThreadId (Windows API)
  - GetTickCount (Windows API)
  - CreateFile (Windows API)
  - CloseHandle (Windows API)
  - [write_stderr](../w/write_stderr.md)

- Called from (representative examples):
  - [pgwin32_install_crashdump_handler](../p/pgwin32_install_crashdump_handler.md) (registered as exception handler)

## Notes and Other Information
- Only creates crash dumps if the "crashdumps" directory exists in the PostgreSQL data directory
- Requires dbghelp.dll to be available on the system
- Generated dump files are named with pattern "postgres-pid[PID]-[TICKS].mdmp"
- The minidump type varies based on the dbghelp.dll version available
- Runs in crash context so memory allocation and PostgreSQL function usage must be avoided
- Always returns EXCEPTION_CONTINUE_SEARCH to allow normal Windows exception processing
- Uses different dump types depending on dbghelp.dll version (checks for EnumDirTree function to detect version 5.2+)