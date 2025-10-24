# init_parallel_dump_utils

## Location
[src/bin/pg_dump/parallel.c:236-263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L236-L263)

## Overview
Initializes parallel dump support functionality and should be called early in process startup, regardless of whether parallel activity is intended.

## Definition

```c
void
init_parallel_dump_utils(void)
```
## Detailed Description
This function performs platform-specific initialization required for parallel dump operations in pg_dump and pg_restore utilities. On Windows platforms, it:

1. Allocates a Thread Local Storage (TLS) index for thread-specific data management
2. Records the main thread ID for later reference
3. Initializes Windows Socket API (WSA) for network communication with version 2.2
4. Sets a flag to prevent redundant initialization calls

The function uses conditional compilation (#ifdef WIN32) and only performs actual work on Windows systems. On other platforms, the function effectively does nothing. This design allows the same code to work across different operating systems while handling Windows-specific requirements for threaded socket operations.

The initialization is protected by a static flag  to ensure it only runs once per process, making it safe to call multiple times.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - TlsAlloc (Windows API)
  - GetCurrentThreadId (Windows API)
  - WSAStartup (Windows Socket API)
  - MAKEWORD (Windows macro)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting)
  - [ParallelSlot](../P/ParallelSlot.md) (type reference)

- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:466)
  - [main](../m/main.md) (in src/bin/pg_dump/pg_restore.c:139)

## Notes and Other Information
- This function is Windows-specific but designed to be called on all platforms
- Must be called early in process startup before any parallel dump operations
- Uses static variables to maintain state and prevent duplicate initialization
- The WSA initialization is required for socket operations in threaded Windows applications
- Error handling includes fatal error reporting if WSAStartup fails

## Simplified Source

```c
void init_parallel_dump_utils(void) {
#ifdef WIN32
    if (!parallel_init_done) {
        WSADATA wsaData;
        int err;

        // Prepare for threaded operation
        tls_index = TlsAlloc();
        mainThreadId = GetCurrentThreadId();

        // Initialize Windows socket access
        err = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (err != 0)
            pg_fatal("%s() failed: error code %d", "WSAStartup", err);

        parallel_init_done = true;
    }
#endif
    // No-op on non-Windows platforms
}
```