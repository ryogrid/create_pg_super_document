# pgkill

## Location
[src/port/kill.c:22-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/kill.c#L22-L97)

## Overview
The  function is PostgreSQL's Windows-specific implementation of the POSIX  system call, providing signal sending functionality between processes on Windows platforms where native signal support is limited.

## Definition

```c
int
pgkill(int pid, int sig)
```
## Detailed Description
The  function implements signal delivery on Windows by using named pipes for inter-process communication. Since Windows lacks native POSIX signal support, PostgreSQL creates a custom signaling mechanism where processes listen on named pipes for signal notifications.

The function handles two distinct cases:
1. **SIGKILL signals**: Directly terminates the target process using Windows  API
2. **Other signals**: Sends signal data through a named pipe to the target process

The implementation creates a named pipe with the pattern  and attempts to communicate with the target process. The target process must be running PostgreSQL code that sets up a corresponding named pipe server to receive these signals.

## Parameters / Member Variables
- `pid`: Process ID of the target process to send the signal to (must be > 0)
- `sig`: Signal number to send (must be >= 0 and < PG_SIGNAL_COUNT)
## Dependencies
- Functions called/Symbols referenced:
  -  (constant defining maximum signal number)
  -  (signal constant for process termination)
  -  (function to map Windows errors to errno values)
  - Windows API functions: , , , , 

- Called from (representative examples):
  -  (macro definition in )

## Notes and Other Information
- This function is Windows-specific and located in the port compatibility layer
- Returns 0 on success, -1 on failure (following POSIX  conventions)
- Sets  appropriately for different error conditions:
  - : Invalid signal number or PID
  - : Target process not found or pipe not available
  - : Access denied to target process
- Special handling for transient pipe errors (ERROR_BROKEN_PIPE, ERROR_BAD_PIPE) treats them as successful operations, similar to how POSIX handles zombie processes
- Signal 0 is allowed but will be ignored by the receiving process (used for process existence checking)
- Process groups (pid <= 0) are not supported, unlike POSIX 