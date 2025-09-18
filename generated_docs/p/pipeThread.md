# pipeThread

## Location
src/backend/postmaster/syslogger.c: 1141 - 1217

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
  - _dosmaperr
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