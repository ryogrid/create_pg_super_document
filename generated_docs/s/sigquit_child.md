# sigquit_child

## Location
[src/backend/postmaster/postmaster.c:3452-3465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3452-L3465)

## Overview
A convenience function that sends either SIGQUIT or SIGABRT to a child process after detecting a crash of another child process in the PostgreSQL postmaster.

## Definition

```c
static void
sigquit_child(pid_t pid)
```
## Detailed Description
This function is designed to terminate child processes in response to crashes in the PostgreSQL system. It serves as a wrapper around signal_child() with enhanced logging and configurable signal selection. The function determines which signal to send based on the global send_abort_for_crash setting - normally SIGQUIT for standard termination, but optionally SIGABRT for developers who want to collect core dumps from each terminated process. The action is logged at DEBUG2 level with details about which signal is being sent to which process.

## Parameters / Member Variables
- `pid`: The process ID of the child process to be terminated
## Dependencies
- Functions called/Symbols referenced:
  - ereport (for logging)
  - [signal_child](signal_child.md) (actual signal sending)
  - send_abort_for_crash (global variable determining signal type)
  - DEBUG2 (logging level constant)
  - SIGQUIT/SIGABRT (signal constants)
- Called from (representative examples):
  - [HandleChildCrash](../H/HandleChildCrash.md) (multiple locations for different child process types)

## Notes and Other Information
- This is a static function internal to postmaster.c
- The choice between SIGQUIT and SIGABRT is controlled by the send_abort_for_crash global variable
- Used extensively in HandleChildCrash function to terminate various types of child processes (backends, autovacuum workers, background writers, etc.)
- The higher logging level (DEBUG2) helps with debugging child process termination issues
- Most comments in the postmaster code assume SIGQUIT is used, but SIGABRT support exists for development debugging

## Simplified Source

```c
// Simplified version of sigquit_child
static void sigquit_child(pid_t pid) {
    // Choose signal based on configuration: SIGABRT for core dumps, SIGQUIT for normal termination
    int signal_to_send = send_abort_for_crash ? SIGABRT : SIGQUIT;
    const char *signal_name = send_abort_for_crash ? "SIGABRT" : "SIGQUIT";

    // Log the termination action at debug level
    ereport(DEBUG2,
            (errmsg_internal("sending %s to process %d", signal_name, (int) pid)));

    // Send the appropriate termination signal to the child process
    signal_child(pid, signal_to_send);
}
```

Key simplifications made:
- Extracted signal selection logic into clear variables
- Added explanatory comments for signal choice rationale
- Simplified the conditional logic for better readability
- Maintained the essential logging and signal sending functionality
- Clarified the purpose of the send_abort_for_crash flag