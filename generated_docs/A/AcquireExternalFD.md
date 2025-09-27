# AcquireExternalFD

## Location
[src/backend/storage/file/fd.c:1183-1217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1183-L1217)

## Overview
Attempts to reserve an external file descriptor with built-in resource limits, providing safe acquisition of file descriptors that will be held for extended periods outside PostgreSQL's VFD system.

## Definition
```c
bool AcquireExternalFD(void)
```

## Detailed Description
AcquireExternalFD provides a controlled mechanism for reserving file descriptors that will be used by external libraries or for purposes outside PostgreSQL's managed Virtual File Descriptor (VFD) system. It implements a safety limit to prevent external FD usage from consuming too many of PostgreSQL's available file descriptors.

The function enforces a conservative limit: no more than max_safe_fds / 3 file descriptors can be reserved for external use. This ensures that PostgreSQL retains sufficient file descriptors for its own operations while allowing controlled access to external FD usage.

Key characteristics:
- Returns immediately without blocking
- Enforces resource limits to prevent FD exhaustion  
- Sets errno to EMFILE when the limit is exceeded
- Integrates with PostgreSQL's global FD tracking via ReserveExternalFD()

This function should be used when the total number of external FDs needed is unpredictable or potentially large, as it provides protection against resource exhaustion.

## Parameters / Member Variables
- No parameters (void function)
- Returns: true if FD successfully reserved, false if limit exceeded

## Dependencies
- Functions called/Symbols referenced:
  - [ReserveExternalFD](../R/ReserveExternalFD.md)
  - Global variables: numExternalFDs, max_safe_fds
- Called from (representative examples):
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [libpqsrv_connect_prepare](../l/libpqsrv_connect_prepare.md)

## Notes and Other Information
- Implements a "one-third rule" - limits external FDs to max_safe_fds / 3
- Caller must pair successful calls with ReleaseExternalFD() when done
- Sets errno to EMFILE on failure to mimic system behavior
- Designed for cases where FD requirements are unpredictable and potentially large
- Contrast with ReserveExternalFD() which doesn't enforce limits
- Used by wait event systems and connection management code
- Critical for preventing external FD usage from starving PostgreSQL's own operations
- The 1/3 limit ensures PostgreSQL retains 2/3 of available FDs for database operations

## Simplified Source

```c
// Simplified version of AcquireExternalFD
bool AcquireExternalFD(void) {
    // Check if we're within the safe limit (max 1/3 of available FDs for external use)
    if (numExternalFDs < max_safe_fds / 3) {
        // Reserve the FD and update global counters
        ReserveExternalFD();
        return true;
    }

    // Limit exceeded - set error and fail
    errno = EMFILE;
    return false;
}
```

Key simplifications made:
- Added clear comments explaining the one-third rule logic
- Simplified the conditional check with explanatory comment
- Abstracted the ReserveExternalFD() call details
- Focused on the main decision logic: check limit, reserve or fail
- Preserved the essential error handling with EMFILE errno