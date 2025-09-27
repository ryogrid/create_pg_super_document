# sigUsr1Handler

## Location
[src/backend/postmaster/syslogger.c:1594-1598](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1594-L1598)

## Overview
A signal handler function that processes SIGUSR1 signals to initiate log file rotation in the PostgreSQL syslogger process.

## Definition
```c
static void sigUsr1Handler(SIGNAL_ARGS)
```
Where SIGNAL_ARGS expands to `int postgres_signal_arg`.

## Detailed Description
This function serves as the signal handler for SIGUSR1 in the syslogger process. When the syslogger receives a SIGUSR1 signal, this handler is invoked to request log rotation. The function sets a global flag to indicate that rotation has been requested and wakes up the main syslogger loop by setting a latch.

The handler is designed to be async-signal-safe and performs minimal operations:
1. Sets the rotation_requested flag to true
2. Sets the MyLatch to wake up the main event loop

The actual log rotation is performed later by the main syslogger loop when it detects that rotation_requested is true.

## Parameters / Member Variables
- `postgres_signal_arg`: Standard signal handler parameter (though not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md) (wakes up the main syslogger event loop)
  - SIGNAL_ARGS (macro that expands to `int postgres_signal_arg`)
- Global variables modified:
  - rotation_requested (volatile sig_atomic_t flag set to true)
  - MyLatch (set to wake up the main loop)
- Called from (signal handling):
  - Registered as SIGUSR1 handler in SysLoggerMain (src/backend/postmaster/syslogger.c:286)

## Notes and Other Information
- This is a static function, only accessible within the syslogger.c file
- The function is registered as the SIGUSR1 handler using pqsignal() in SysLoggerMain
- Uses async-signal-safe operations only (setting atomic variables and latches)
- The rotation_requested variable is declared as volatile sig_atomic_t for thread safety
- SIGUSR1 is commonly used in PostgreSQL for requesting log rotation from external tools
- The actual rotation logic is handled in the main syslogger loop, not in this signal handler
- This follows PostgreSQL's pattern of keeping signal handlers minimal and deferring work to the main event loop

## Simplified Source

```c
// Simplified version of sigUsr1Handler
static void sigUsr1Handler(SIGNAL_ARGS) {
    // Step 1: Mark that log rotation is needed
    rotation_requested = true;

    // Step 2: Wake up the main syslogger loop
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added explanatory comments for each operation
- Highlighted the two-step process: flag setting and latch signaling
- Maintained the original logic while making the purpose clearer