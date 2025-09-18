# sigpipe_info

## Location
src/interfaces/libpq/fe-secure.c: 57 - 63

## Overview
A structure used in PostgreSQL's libpq library to manage SIGPIPE signal handling during network operations on Unix-like systems.

## Definition
```c
struct sigpipe_info
{
    sigset_t    oldsigmask;
    bool        sigpipe_pending;
    bool        got_epipe;
};
```

## Detailed Description
The `sigpipe_info` structure is a core component of PostgreSQL's libpq SIGPIPE management system for Unix-like platforms. It stores the state information necessary to properly handle SIGPIPE signals during network I/O operations. This structure is used in conjunction with macros like `DISABLE_SIGPIPE`, `REMEMBER_EPIPE`, and `RESTORE_SIGPIPE` to temporarily block SIGPIPE signals, track EPIPE errors, and restore the original signal mask after network operations complete.

The structure is only compiled on non-Windows platforms (controlled by `#ifndef WIN32`), as Windows does not use SIGPIPE signals for broken pipe conditions.

## Parameters / Member Variables
- `oldsigmask`: Stores the previous signal mask before SIGPIPE was blocked, allowing restoration of the original signal handling state
- `sigpipe_pending`: Boolean flag indicating whether a SIGPIPE signal was pending when signal blocking was initiated
- `got_epipe`: Boolean flag tracking whether an EPIPE error occurred during the protected network operation

## Dependencies
- Functions called/Symbols referenced:
  - sigset_t (POSIX signal set type)
- Called from (representative examples):
  - Used through macros: DECLARE_SIGPIPE_INFO, DISABLE_SIGPIPE, REMEMBER_EPIPE, RESTORE_SIGPIPE
  - Utilized in network I/O operations throughout libpq

## Notes and Other Information
- This structure is platform-specific and only exists on Unix-like systems (excluded on Windows via preprocessor directives)
- Part of a comprehensive SIGPIPE handling system that prevents the application from being terminated by broken pipe signals during network operations
- Works in conjunction with `pq_block_sigpipe()` and `pq_reset_sigpipe()` functions for complete signal management
- The structure is typically declared as a local variable using the `DECLARE_SIGPIPE_INFO` macro rather than being directly instantiated