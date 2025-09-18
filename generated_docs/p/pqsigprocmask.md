# pqsigprocmask

## Location
src/backend/port/win32/signal.c: 171 - 209

## Overview
pqsigprocmask is the Windows-specific implementation of the POSIX sigprocmask function for examining and changing the signal mask.

## Definition
```c
int pqsigprocmask(int how, const sigset_t *set, sigset_t *oset)
```

## Detailed Description
This function provides a Windows implementation of the POSIX sigprocmask system call, allowing PostgreSQL to manage signal blocking in a portable way. It operates on the global signal mask (pg_signal_mask) and supports all standard POSIX signal masking operations.

The function performs the following operations:
1. **Old Mask Retrieval**: If oset is provided, stores the current signal mask
2. **Mask Modification**: Applies the requested mask operation based on the 'how' parameter
3. **Signal Dispatch**: After modifying the mask, dispatches any previously queued signals that may now be unblocked

Key behaviors:
- Only operates on the main thread (no synchronization required)
- Immediately dispatches newly unblocked signals
- Follows POSIX semantics for signal mask manipulation
- Returns appropriate error codes for invalid operations

## Parameters / Member Variables
- `how`: Specifies the operation to perform on the signal mask (SIG_BLOCK, SIG_UNBLOCK, or SIG_SETMASK)
- `set`: Pointer to the signal set to apply to the current mask (can be NULL)
- `oset`: Pointer to store the previous signal mask value (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - sigset_t (signal set type)
  - SIG_BLOCK (add signals to mask)
  - SIG_UNBLOCK (remove signals from mask) 
  - SIG_SETMASK (replace entire mask)
  - pgwin32_dispatch_queued_signals (dispatch unblocked signals)
  - EINVAL (errno constant)
- Called from (representative examples):
  - sigaction (via header mapping)
  - sigprocmask (via header mapping)

## Notes and Other Information
- This is a Windows-specific implementation located in src/backend/port/win32/signal.c
- The function is mapped to the standard sigprocmask name via preprocessor macros in header files
- Only intended for use on the main thread; no synchronization is performed
- After any mask modification, the function proactively dispatches queued signals that may have become unblocked
- Returns 0 on success, -1 on error (with errno set to EINVAL for invalid 'how' parameter)
- Provides POSIX-compatible signal masking semantics on Windows platforms
- The signal mask is stored in the global variable pg_signal_mask