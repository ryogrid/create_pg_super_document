# pg_signal_thread

## Location
[src/backend/port/win32/signal.c:274-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/win32/signal.c#L274-L376)

## Overview
The main signal handling thread function for PostgreSQL on Windows that listens for signal messages through a named pipe and queues them for the main thread.

## Definition

```c
static DWORD WINAPI
pg_signal_thread(LPVOID param)
```
## Detailed Description
This function implements the core signal handling mechanism for PostgreSQL on Windows. It runs as a separate thread that continuously listens for incoming signal messages through a named pipe. The function creates and manages a named pipe with a process-specific name, waits for client connections, reads signal numbers from connected clients, and queues the signals for processing by the main thread.

The function operates in an infinite loop, handling pipe creation, client connections, signal reading, and pipe management. It provides robust error handling and recovery mechanisms, including automatic pipe recreation on failure. The function ensures proper synchronization by responding to signal senders only after the signal has been queued, providing stronger guarantees than POSIX signals.

## Parameters / Member Variables
- `param`: A generic parameter passed to the thread function (LPVOID type), not used in this implementation.
## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - GetCurrentProcessId (Windows API)
  - CreateNamedPipe (Windows API)
  - [write_stderr](../w/write_stderr.md)
  - SleepEx (Windows API)
  - ConnectNamedPipe (Windows API)
  - GetLastError (Windows API)
  - ReadFile (Windows API)
  - [pg_queue_signal](pg_queue_signal.md)
  - WriteFile (Windows API)
  - FlushFileBuffers (Windows API)
  - DisconnectNamedPipe (Windows API)
  - CloseHandle (Windows API)
- Global variables accessed:
  - pgwin32_initial_signal_pipe
- Called from (representative examples):
  - [pgwin32_signal_initialize](pgwin32_signal_initialize.md) (in signal.c:103)

## Notes and Other Information
- Runs as a Windows thread with WINAPI calling convention
- Uses a named pipe with pattern: 
- Starts with pgwin32_initial_signal_pipe if available, otherwise creates a new pipe
- Pipe configuration: PIPE_ACCESS_DUPLEX, PIPE_TYPE_MESSAGE, PIPE_READMODE_MESSAGE
- Implements retry logic with 500ms delay when pipe creation fails
- Handles the Windows quirk where ERROR_PIPE_CONNECTED indicates successful connection
- Provides stronger ordering guarantees than POSIX by queueing signals before responding
- Includes comprehensive error handling and automatic recovery mechanisms
- Maintains pipe connection lifecycle: connect → read → queue → respond → flush → disconnect
- Returns 0 on exit (though the function runs in an infinite loop)
- Critical for PostgreSQL's signal emulation system on Windows platforms

## Simplified Source

```c
// Simplified version of pg_signal_thread
static DWORD WINAPI pg_signal_thread(LPVOID param) {
    char pipename[128];
    HANDLE pipe = pgwin32_initial_signal_pipe;

    // Set up process-specific pipe name
    snprintf(pipename, sizeof(pipename), "\\\\.\\pipe\\pgsignal_%lu", GetCurrentProcessId());

    // Main signal listening loop
    for (;;) {
        // Create new pipe if needed
        if (pipe == INVALID_HANDLE_VALUE) {
            pipe = CreateNamedPipe(pipename, PIPE_ACCESS_DUPLEX,
                                 PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
                                 PIPE_UNLIMITED_INSTANCES, 16, 16, 1000, NULL);

            if (pipe == INVALID_HANDLE_VALUE) {
                write_stderr("could not create signal listener pipe: error code %lu; retrying\n", GetLastError());
                SleepEx(500, FALSE);
                continue;
            }
        }

        // Wait for client connection (handle Windows quirk where ERROR_PIPE_CONNECTED = success)
        BOOL connected = ConnectNamedPipe(pipe, NULL) ? TRUE : (GetLastError() == ERROR_PIPE_CONNECTED);

        if (connected) {
            // Read signal number from client
            BYTE sigNum;
            DWORD bytes;

            if (ReadFile(pipe, &sigNum, 1, &bytes, NULL) && bytes == 1) {
                // Queue signal before responding to ensure ordering
                pg_queue_signal(sigNum);

                // Respond to client to allow their call to complete
                WriteFile(pipe, &sigNum, 1, &bytes, NULL);
                FlushFileBuffers(pipe);
            }

            // Disconnect to reuse pipe
            DisconnectNamedPipe(pipe);
        } else {
            // Connection failed - close and recreate pipe
            CloseHandle(pipe);
            pipe = INVALID_HANDLE_VALUE;
        }
    }
    return 0;
}
```

Key simplifications made:
- Removed detailed error handling comments for brevity
- Consolidated variable declarations where appropriate
- Simplified the connection logic explanation
- Focused on the main execution flow
- Retained critical Windows-specific handling and ordering guarantees
- Preserved the essential signal processing workflow