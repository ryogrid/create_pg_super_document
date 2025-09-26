# AcquireExternalFD

## Location
src/backend/storage/file/fd.c: 1183 - 1217

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
  - ReserveExternalFD
  - Global variables: numExternalFDs, max_safe_fds
- Called from (representative examples):
  - CreateWaitEventSet
  - libpqsrv_connect_prepare

## Notes and Other Information
- Implements a "one-third rule" - limits external FDs to max_safe_fds / 3
- Caller must pair successful calls with ReleaseExternalFD() when done
- Sets errno to EMFILE on failure to mimic system behavior
- Designed for cases where FD requirements are unpredictable and potentially large
- Contrast with ReserveExternalFD() which doesn't enforce limits
- Used by wait event systems and connection management code
- Critical for preventing external FD usage from starving PostgreSQL's own operations
- The 1/3 limit ensures PostgreSQL retains 2/3 of available FDs for database operations