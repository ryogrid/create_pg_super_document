# pq_block_sigpipe

## Location
[src/interfaces/libpq/fe-secure.c:519-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L519-L568)

## Overview
Blocks the SIGPIPE signal for the current thread and detects any pending SIGPIPE signals to prevent network write operations from terminating the application unexpectedly.

## Definition
```c
int pq_block_sigpipe(sigset_t *osigset, bool *sigpipe_pending)
```

## Detailed Description
This function implements signal handling for SIGPIPE to ensure that network write operations (such as send() and write()) do not cause the application to terminate when writing to a broken pipe or closed socket connection. SIGPIPE is generated when attempting to write to a pipe or socket that has been closed by the peer, and by default would terminate the process.

The function blocks SIGPIPE for the current thread using pthread_sigmask() and saves the previous signal mask for later restoration. It also checks if SIGPIPE was already blocked before the call, and if so, determines whether there was already a pending SIGPIPE signal that needs to be handled.

This is a critical function for robust network communication in libpq, ensuring that connection failures are handled gracefully through return codes rather than process termination.

## Parameters / Member Variables
- `osigset`: Output parameter that receives the previous signal mask before SIGPIPE was blocked. This mask is used later by pq_reset_sigpipe() to restore the original signal state.
- `sigpipe_pending`: Output parameter that indicates whether a SIGPIPE signal was already pending before this function was called. Set to true if a SIGPIPE was pending, false otherwise.

## Dependencies
- Functions called/Symbols referenced:
  - sigemptyset (initialize empty signal set)
  - sigaddset (add SIGPIPE to signal set) 
  - SIGPIPE (signal constant)
  - pthread_sigmask (block signals for current thread)
  - SIG_BLOCK (signal mask operation constant)
  - SOCK_ERRNO_SET, SOCK_ERRNO (libpq error handling macros)
  - sigismember (check if signal is in set)
  - sigpending (get pending signals)
- Called from (representative examples):
  - DISABLE_SIGPIPE macro (fe-secure.c:71) - used throughout libpq for safe network I/O
  - [fe](../f/fe.md)-print.c:186 - [when](../w/when.md) printing query results that may involve network I/O

## Notes and Other Information
- Returns 0 on success, -1 on failure (if pthread_sigmask() or sigpending() fails)
- Only available on non-Windows platforms (guarded by !defined(WIN32))
- Must be paired with pq_reset_sigpipe() to restore the original signal mask
- The function is thread-safe and only affects the calling thread's signal mask
- Used as part of the DISABLE_SIGPIPE/RESTORE_SIGPIPE macro pattern in libpq
- Essential for preventing connection errors from terminating client applications
- The sigpipe_pending detection is important for proper signal handling when SIGPIPE was already blocked
- Part of libpq's robust error handling strategy for network communication failures

## Simplified Source

```c
int pq_block_sigpipe(sigset_t *osigset, bool *sigpipe_pending) {
    sigset_t sigpipe_sigset;
    sigset_t sigset;

    // Create signal set containing only SIGPIPE
    sigemptyset(&sigpipe_sigset);
    sigaddset(&sigpipe_sigset, SIGPIPE);

    // Block SIGPIPE and save previous mask
    SOCK_ERRNO_SET(pthread_sigmask(SIG_BLOCK, &sigpipe_sigset, osigset));
    if (SOCK_ERRNO)
        return -1;

    // Check if SIGPIPE was already blocked and pending
    if (sigismember(osigset, SIGPIPE)) {
        // Check for pending SIGPIPE signal
        if (sigpending(&sigset) != 0)
            return -1;

        *sigpipe_pending = sigismember(&sigset, SIGPIPE);
    } else {
        *sigpipe_pending = false;
    }

    return 0;
}
```