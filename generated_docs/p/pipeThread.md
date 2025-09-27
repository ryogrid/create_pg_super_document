# pipeThread

## Location
[src/backend/postmaster/syslogger.c:1141-1217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1141-L1217)

## Overview
A Windows-specific worker thread that transfers data from the syslog pipe to the current logfile, working around Windows pipe handling limitations.

## Definition
```c
static unsigned int __stdcall pipeThread(void *arg)
```

## Detailed Description
This function serves as a dedicated worker thread on Windows systems to handle continuous data transfer from the syslog pipe to log files. It exists because Windows' WaitForMultipleObjects does not work properly with unnamed pipes (always reports "signaled"), and select() only works with sockets, making it impossible to efficiently wait for both pipe input and signals like SIGHUP in the main thread.

The thread runs in an infinite loop, continuously reading data from the pipe using ReadFile(). It uses a critical section (sysloggerSection) to synchronize access to shared global state with the main thread, particularly for operations that use palloc()/pfree(). When data is successfully read, it processes the input through process_pipe_input(). The thread also monitors log file sizes and triggers rotation by setting a latch when files exceed the configured rotation size.

The thread terminates when it detects EOF on the pipe (ERROR_HANDLE_EOF or ERROR_BROKEN_PIPE), at which point it flushes any remaining buffered data and signals the main thread to quit.

## Parameters / Member Variables
- `arg`: Void pointer argument (currently unused in the function)

## Dependencies
- Functions called/Symbols referenced:
  - READ_BUF_SIZE
  - [_dosmaperr](../d/_dosmaperr.md)
  - [process_pipe_input](process_pipe_input.md)
  - [SetLatch](../S/SetLatch.md)
  - [flush_pipe_input](../f/flush_pipe_input.md)
- Called from (representative examples):
  - [SysLoggerMain](../S/SysLoggerMain.md)

## Notes and Other Information
- Windows-specific function using __stdcall calling convention
- Uses Win32 API functions like ReadFile, EnterCriticalSection, LeaveCriticalSection
- Critical section synchronization prevents race conditions with main thread
- Handles log rotation size monitoring for all log file types (syslog, CSV, JSON)
- Terminates thread execution with _endthread() upon pipe EOF
- Essential for Windows syslogger functionality due to platform limitations

## Simplified Source

```c
// Simplified version of pipeThread
static unsigned int __stdcall pipeThread(void *arg) {
    char logbuffer[READ_BUF_SIZE];
    int bytes_in_logbuffer = 0;

    // Main processing loop: continuously read from pipe
    for (;;) {
        DWORD bytesRead;
        BOOL result;

        // Read data from the syslog pipe
        result = ReadFile(syslogPipe[0],
                         logbuffer + bytes_in_logbuffer,
                         sizeof(logbuffer) - bytes_in_logbuffer,
                         &bytesRead, 0);

        // Synchronize with main thread before touching shared state
        EnterCriticalSection(&sysloggerSection);

        if (!result) {
            DWORD error = GetLastError();
            // Check for pipe closure (EOF or broken pipe)
            if (error == ERROR_HANDLE_EOF || error == ERROR_BROKEN_PIPE)
                break;
            // Log read errors but continue
            ereport(LOG, (errcode_for_file_access(),
                         errmsg("could not read from logger pipe: %m")));
        }
        else if (bytesRead > 0) {
            // Process the data we successfully read
            bytes_in_logbuffer += bytesRead;
            process_pipe_input(logbuffer, &bytes_in_logbuffer);
        }

        // Check if log rotation is needed based on file size
        if (Log_RotationSize > 0) {
            if (any_logfile_exceeds_rotation_size()) {
                SetLatch(MyLatch);  // Signal main thread for rotation
            }
        }

        LeaveCriticalSection(&sysloggerSection);
    }

    // Cleanup when pipe closes: flush remaining data and signal shutdown
    pipe_eof_seen = true;
    flush_pipe_input(logbuffer, &bytes_in_logbuffer);
    SetLatch(MyLatch);  // Wake main thread to quit

    LeaveCriticalSection(&sysloggerSection);
    _endthread();
    return 0;
}
```

Key simplifications made:
- Abstracted the complex log rotation size check into a conceptual function call
- Consolidated error handling logic while preserving the essential pipe EOF detection
- Added high-level comments explaining each major step
- Simplified the ReadFile parameters presentation for clarity
- Focused on the main execution flow: read → process → check rotation → repeat
- Preserved the critical section synchronization which is essential for thread safety