# pq_reset_sigpipe

## Location
[src/interfaces/libpq/fe-secure.c:569-596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L569-L596)

## Overview
This function handles the cleanup of SIGPIPE signals and restores the original signal mask after socket operations in PostgreSQL's libpq client library.

## Definition

```c
void
pq_reset_sigpipe(sigset_t *osigset, bool sigpipe_pending, bool got_epipe)
```
## Detailed Description
The  function is responsible for cleaning up SIGPIPE signal handling after socket operations that may have generated such signals. It performs two main operations:

1. **SIGPIPE Cleanup**: If an EPIPE error occurred and no SIGPIPE was already pending, it checks for and discards any pending SIGPIPE signals to prevent them from affecting subsequent operations.

2. **Signal Mask Restoration**: Restores the original signal mask that was saved before blocking SIGPIPE signals.

The function is designed to be safe regarding errno preservation - it saves and restores the socket errno value to ensure that error codes from preceding operations (like send()) are not lost. The implementation assumes that the C library doesn't queue multiple SIGPIPE events, which is a reasonable assumption for most systems.

## Parameters / Member Variables
- : Pointer to the original signal set that should be restored
- : Boolean indicating whether a SIGPIPE signal was already pending before the operation
- : Boolean indicating whether an EPIPE error occurred (or might have occurred) during the operation

## Dependencies
- Functions called/Symbols referenced:
  - SOCK_ERRNO (macro for getting socket errno)
  - sigpending (POSIX signal function)
  - sigismember (POSIX signal set function)
  - sigemptyset (POSIX signal set function)
  - sigaddset (POSIX signal set function)
  - sigwait (POSIX signal function)
  - pthread_sigmask (POSIX thread signal mask function)
  - SOCK_ERRNO_SET (macro for setting socket errno)
  - SIGPIPE (signal constant)
  - SIG_SETMASK (signal mask operation constant)

- Called from (representative examples):
  - This function is typically called after socket write operations that may generate SIGPIPE
  - Used in conjunction with pq_block_sigpipe() for complete SIGPIPE handling

## Notes and Other Information
- The function preserves errno values to avoid masking errors from socket operations
- It assumes the C library doesn't queue multiple SIGPIPE events
- The caller should set got_epipe = false if certain no EPIPE error occurred, allowing the function to skip the signal clearing operation
- This is part of PostgreSQL's libpq client library's secure connection handling
- The function is located in src/interfaces/libpq/fe-secure.c (lines 569-596)
- Should be used as part of a pair with pq_block_sigpipe() for proper SIGPIPE handling around socket operations