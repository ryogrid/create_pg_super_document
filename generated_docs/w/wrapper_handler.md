# wrapper_handler

## Location
[src/port/pqsignal.c:86-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pqsignal.c#L86-L134)

## Overview
A signal wrapper handler function that acts as an intermediate handler for all signals set up by pqsignal(), ensuring proper signal handling within PostgreSQL processes while protecting against modifications from child processes.

## Definition
```c
static void wrapper_handler(SIGNAL_ARGS)
```

## Detailed Description
The wrapper_handler function serves as a protective wrapper around user-provided signal handlers in PostgreSQL. When pqsignal() is called with a signal handler (not SIG_IGN or SIG_DFL), it actually registers wrapper_handler as the system-level signal handler, while storing the users original handler in the pqsignal_handlers array.

The wrapper performs several important functions:
1. Preserves and restores the errno value across signal handling
2. Validates that the signal is being handled in the correct process (not a child process)
3. Ensures that only processes that set MyProcPid properly can modify shared memory
4. Falls back to default signal handling if process validation fails
5. Calls the original user-provided signal handler if validation succeeds

## Parameters / Member Variables
- Uses `SIGNAL_ARGS` macro for signal number parameter
- Accesses global variables: `MyProcPid`, `PostmasterPid`, `postgres_signal_arg`
- References `pqsignal_handlers` array for stored handler functions

## Dependencies
- Functions called/Symbols referenced:
  - `getpid()` - Get current process ID
  - `pqsignal()` - Reset signal handler to default
  - `raise()` - Re-raise the signal
- Global variables accessed:
  - `MyProcPid` - Expected process ID
  - `PostmasterPid` - Postmaster process ID
  - `postgres_signal_arg` - Signal number
  - `pqsignal_handlers[]` - Array of user signal handlers

## Notes and Other Information
- This is a static function, only visible within the pqsignal.c compilation unit
- Provides protection against child processes modifying shared memory
- Ensures signal handling context is appropriate for PostgreSQL processes
- The FRONTEND macro excludes process validation for frontend applications
- Critical for maintaining PostgreSQL's signal handling safety

## Simplified Source

```c
static void wrapper_handler(SIGNAL_ARGS) {
    int saved_errno = errno;

    // Validate we're in the correct process (backend only)
    #ifndef FRONTEND
        // Ensure signal handling occurs in the expected process
        if (unlikely(MyProcPid != (int) getpid())) {
            // Reset to default handler and re-raise signal
            pqsignal(postgres_signal_arg, SIG_DFL);
            raise(postgres_signal_arg);
            return;
        }
    #endif

    // Call the original user-provided signal handler
    (*pqsignal_handlers[postgres_signal_arg])(postgres_signal_arg);

    // Restore errno to its original value
    errno = saved_errno;
}
```